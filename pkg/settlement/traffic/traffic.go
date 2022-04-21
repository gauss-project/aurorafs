package traffic

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"time"

	"math/big"
	"sync"
)

var (
	ErrUnknownBeneficary = errors.New("unknown beneficiary for peer")
	ErrInsufficientFunds = errors.New("insufficient token balance")
)

type SendChequeFunc func(cheque *chequePkg.SignedCheque) error

type Traffic struct {
	sync.Mutex
	trafficPeerBalance    *big.Int
	retrieveChainTraffic  *big.Int
	transferChainTraffic  *big.Int
	retrieveChequeTraffic *big.Int
	transferChequeTraffic *big.Int
	retrieveTraffic       *big.Int
	transferTraffic       *big.Int
	status                CashStatus
}

type TrafficPeer struct {
	trafficLock  sync.Mutex
	trafficPeers map[string]*Traffic
	balance      *big.Int
	totalPaidOut *big.Int
}

type TrafficCheque struct {
	Peer                boson.Address `json:"peer"`
	OutstandingTraffic  *big.Int      `json:"outstandingTraffic"`
	SentSettlements     *big.Int      `json:"sentSettlements"`
	ReceivedSettlements *big.Int      `json:"receivedSettlements"`
	Total               *big.Int      `json:"total"`
	Uncashed            *big.Int      `json:"unCashed"`
	Status              CashStatus    `json:"status"`
}

type CashStatus = int

const (
	UnOperation CashStatus = iota
	Operation
)

type cashCheque struct {
	txHash       common.Hash
	peer         boson.Address
	chainAddress common.Address
}

type CashOutStatus struct {
	Overlay boson.Address `json:"overlay"`
	Status  bool          `json:"status"`
}

type TrafficInfo struct {
	Balance          *big.Int `json:"balance"`
	AvailableBalance *big.Int `json:"availableBalance"`
	TotalSendTraffic *big.Int `json:"totalSendTraffic"`
	ReceivedTraffic  *big.Int `json:"receivedTraffic"`
}

type ApiInterface interface {
	LastSentCheque(peer boson.Address) (*chequePkg.Cheque, error)

	LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error)

	CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error)

	TrafficCheques() ([]*TrafficCheque, error)

	Address() common.Address

	TrafficInfo() (*TrafficInfo, error)

	TrafficInit() error

	API() rpc.API
}

const (
	trafficChainRefreshDuration = 24 * time.Hour
)

type Service struct {
	logger              logging.Logger
	chainAddress        common.Address
	store               storage.StateStorer
	metrics             metrics
	chequeStore         chequePkg.ChequeStore
	cashout             chequePkg.CashoutService
	trafficChainService chain.Traffic
	p2pService          p2p.Service
	peersLock           sync.Mutex
	trafficPeers        TrafficPeer
	addressBook         Addressbook
	chequeSigner        chequePkg.ChequeSigner
	protocol            trafficprotocol.Interface
	notifyPaymentFunc   settlement.NotifyPaymentFunc
	chainID             int64
	pubSubLk            sync.RWMutex
	pubSub              map[string][]chan interface{}
	//txHash:beneficiary
	cashChequeChan chan cashCheque
}

func New(logger logging.Logger, chainAddress common.Address, store storage.StateStorer, trafficChainService chain.Traffic,
	chequeStore chequePkg.ChequeStore, cashout chequePkg.CashoutService, p2pService p2p.Service, addressBook Addressbook,
	chequeSigner chequePkg.ChequeSigner, protocol trafficprotocol.Interface, chainID int64) *Service {

	service := &Service{
		logger:              logger,
		store:               store,
		chainAddress:        chainAddress,
		trafficChainService: trafficChainService,
		metrics:             newMetrics(),
		chequeStore:         chequeStore,
		cashout:             cashout,
		p2pService:          p2pService,
		addressBook:         addressBook,
		chequeSigner:        chequeSigner,
		protocol:            protocol,
		chainID:             chainID,
		trafficPeers: TrafficPeer{
			trafficPeers: make(map[string]*Traffic),
			balance:      big.NewInt(0),
			totalPaidOut: big.NewInt(0),
		},
		pubSub:         make(map[string][]chan interface{}),
		cashChequeChan: make(chan cashCheque, 5),
	}
	service.triggerRefreshInit()
	service.cashChequeReceiptUpdate()
	return service
}

func (s *Service) Init() error {
	err := s.trafficInit()
	if err != nil {
		return err
	}

	return s.addressBook.InitAddressBook()
}

func (s *Service) triggerRefreshInit() {
	ticker := time.NewTicker(trafficChainRefreshDuration)
	go func(t *time.Ticker) {
		for {
			<-t.C
			err := s.trafficInit()
			if err != nil {
				s.logger.Errorf("traffic-InitChain: %w", err)
				//os.Exit(1)
			}
		}
	}(ticker)
}

func newTraffic() *Traffic {
	return &Traffic{
		trafficPeerBalance:    big.NewInt(0),
		retrieveChainTraffic:  big.NewInt(0),
		transferChainTraffic:  big.NewInt(0),
		retrieveChequeTraffic: big.NewInt(0),
		transferChequeTraffic: big.NewInt(0),
		retrieveTraffic:       big.NewInt(0),
		transferTraffic:       big.NewInt(0),
		status:                UnOperation,
	}
}
func NewTrafficInfo() *TrafficInfo {
	return &TrafficInfo{
		Balance:          big.NewInt(0),
		AvailableBalance: big.NewInt(0),
		TotalSendTraffic: big.NewInt(0),
		ReceivedTraffic:  big.NewInt(0),
	}
}

func (s *Service) getTraffic(peer common.Address) (traffic *Traffic) {
	s.trafficPeers.trafficLock.Lock()
	defer s.trafficPeers.trafficLock.Unlock()
	key := peer.String()
	traffic = s.trafficPeers.trafficPeers[key]
	if traffic == nil {
		traffic = newTraffic()
		s.trafficPeers.trafficPeers[key] = traffic
	}
	return
}

func (s *Service) getAllAddress(lastCheques map[common.Address]*chequePkg.Cheque, lastTransCheques map[common.Address]*chequePkg.SignedCheque) (map[common.Address]Traffic, error) {
	chanResp := make(map[common.Address]Traffic)

	retrieveList, err := s.trafficChainService.RetrievedAddress(s.chainAddress)
	if err != nil {
		return chanResp, err
	}
	for _, v := range retrieveList {
		if _, ok := chanResp[v]; !ok {
			chanResp[v] = Traffic{}
		}
	}
	transferList, err := s.trafficChainService.TransferredAddress(s.chainAddress)
	if err != nil {
		return chanResp, err
	}
	for _, v := range transferList {
		if _, ok := chanResp[v]; !ok {
			chanResp[v] = Traffic{}
		}
	}

	for k := range lastCheques {
		if _, ok := chanResp[k]; !ok {
			chanResp[k] = Traffic{}
		}
	}

	for k := range lastTransCheques {
		if _, ok := chanResp[k]; !ok {
			chanResp[k] = Traffic{}
		}
	}

	return chanResp, err
}

func (s *Service) trafficInit() error {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	lastCheques, err := s.chequeStore.LastSendCheques()
	if err != nil {
		s.logger.Errorf("Traffic failed to obtain local check information. ")
		return err
	}

	lastTransCheques, err := s.chequeStore.LastReceivedCheques()
	if err != nil {
		s.logger.Errorf("Traffic failed to obtain local check information. ")
		return err
	}

	addressList, err := s.getAllAddress(lastCheques, lastTransCheques)
	if err != nil {
		return fmt.Errorf("traffic: Failed to get chain node information:%v ", err)
	}

	err = s.replaceTraffic(addressList, lastCheques, lastTransCheques)
	if err != nil {
		return fmt.Errorf("traffic: Update of local traffic data failed. ")
	}

	//transferTotal, err := s.trafficChainService.TransferredTotal(k)
	balance, err := s.trafficChainService.BalanceOf(s.chainAddress)
	if err != nil {
		return fmt.Errorf("failed to get the chain balance")
	}
	s.trafficPeers.balance = balance

	paiOut, err := s.trafficChainService.TransferredTotal(s.chainAddress)
	if err != nil {
		return fmt.Errorf("failed to get the chain totalPaidOut")
	}
	s.trafficPeers.totalPaidOut = paiOut
	return nil
}
func (s *Service) replaceTraffic(addressList map[common.Address]Traffic, lastCheques map[common.Address]*chequePkg.Cheque, lastTransCheques map[common.Address]*chequePkg.SignedCheque) error {

	s.trafficPeers.totalPaidOut = new(big.Int).SetInt64(0)
	workload := make(chan struct{}, 50) // limit goroutine number
	waitGroup := new(sync.WaitGroup)
	for key := range addressList {
		workload <- struct{}{}
		waitGroup.Add(1)
		go func(address common.Address, workload chan struct{}, waitGroup *sync.WaitGroup) {
			defer func() {
				<-workload
				waitGroup.Done()
			}()
			_ = s.getTraffic(address)
			err := s.trafficPeerChainUpdate(address, s.chainAddress)
			if err != nil {
				s.logger.Errorf("traffic: getChainTraffic %v", err.Error())
				return
			}
			err = s.trafficPeerChequeUpdate(address, lastCheques, lastTransCheques)
			if err != nil {
				s.logger.Errorf("traffic: replaceTraffic %v", err.Error())
				return
			}
		}(key, workload, waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func (s *Service) trafficPeerChainUpdate(peerAddress, chainAddress common.Address) error {
	traffic := s.getTraffic(peerAddress)
	traffic.Lock()
	defer traffic.Unlock()
	transferTotal, err := s.trafficChainService.TransAmount(peerAddress, chainAddress)
	if err != nil {
		return err
	}
	retrievedTotal, err := s.trafficChainService.TransAmount(chainAddress, peerAddress)
	if err != nil {
		return err
	}

	traffic.retrieveChainTraffic = retrievedTotal
	traffic.transferChainTraffic = transferTotal
	return nil
}

func (s *Service) trafficPeerChequeUpdate(peerAddress common.Address, lastCheques map[common.Address]*chequePkg.Cheque, lastTransCheques map[common.Address]*chequePkg.SignedCheque) error {

	traffic := s.getTraffic(peerAddress)
	traffic.Lock()
	defer traffic.Unlock()
	traffic.retrieveChequeTraffic = traffic.retrieveChainTraffic
	traffic.retrieveTraffic = traffic.retrieveChainTraffic
	traffic.transferChequeTraffic = traffic.transferChainTraffic
	traffic.transferTraffic = traffic.transferChainTraffic
	if cq, ok := lastCheques[peerAddress]; ok {
		traffic.retrieveTraffic = s.maxBigint(traffic.retrieveTraffic, cq.CumulativePayout)
		traffic.retrieveChequeTraffic = s.maxBigint(traffic.retrieveChequeTraffic, cq.CumulativePayout)
	}

	if cq, ok := lastTransCheques[peerAddress]; ok {
		traffic.transferTraffic = s.maxBigint(traffic.transferTraffic, cq.CumulativePayout)
		traffic.transferChequeTraffic = s.maxBigint(traffic.transferChequeTraffic, cq.CumulativePayout)
	}

	retrieve, err := s.chequeStore.GetRetrieveTraffic(peerAddress)
	if err != nil {
		return err
	}
	traffic.retrieveTraffic = s.maxBigint(traffic.retrieveTraffic, retrieve)

	transfer, err := s.chequeStore.GetTransferTraffic(peerAddress)
	if err != nil {
		return err
	}
	traffic.transferTraffic = s.maxBigint(traffic.transferTraffic, transfer)
	return nil
}

//Returns the maximum value
func (s *Service) maxBigint(a *big.Int, b *big.Int) *big.Int {
	if a.Cmp(b) < 0 {
		return b
	} else {
		return a
	}
}

// LastSentCheque returns the last sent cheque for the peer
func (s *Service) LastSentCheque(peer boson.Address) (*chequePkg.Cheque, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return nil, chequePkg.ErrNoCheque
	}
	return s.chequeStore.LastSendCheque(chainAddress)
}

// LastReceivedCheque returns the list of last received cheques for all peers
func (s *Service) LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return &chequePkg.SignedCheque{}, nil
	}
	return s.chequeStore.LastReceivedCheque(chainAddress)
}

// CashCheque sends a cashing transaction for the last cheque of the peer
func (s *Service) CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return common.Hash{}, chequePkg.ErrNoCheque
	}
	traffic := s.getTraffic(chainAddress)
	c, err := s.cashout.CashCheque(ctx, peer, chainAddress, s.chainAddress)
	if err != nil {
		return common.Hash{}, err
	}
	traffic.updateStatus(Operation)
	s.cashChequeChan <- cashCheque{
		txHash:       c,
		peer:         peer,
		chainAddress: chainAddress,
	}
	return c, err
}

func (s *Service) Address() common.Address {
	return s.chainAddress
}

func (s *Service) TrafficInfo() (*TrafficInfo, error) {
	respTraffic := NewTrafficInfo()
	s.trafficPeers.trafficLock.Lock()
	defer s.trafficPeers.trafficLock.Unlock()
	cashed := big.NewInt(0)
	transfer := big.NewInt(0)
	for _, traffic := range s.trafficPeers.trafficPeers {
		cashed = new(big.Int).Add(cashed, traffic.retrieveChainTraffic)
		transfer = new(big.Int).Add(transfer, traffic.retrieveChequeTraffic)
		respTraffic.TotalSendTraffic = new(big.Int).Add(respTraffic.TotalSendTraffic, traffic.retrieveChequeTraffic)
		respTraffic.ReceivedTraffic = new(big.Int).Add(respTraffic.ReceivedTraffic, traffic.transferChequeTraffic)
	}

	respTraffic.Balance = s.trafficPeers.balance
	respTraffic.AvailableBalance = new(big.Int).Add(respTraffic.Balance, new(big.Int).Sub(cashed, transfer))

	return respTraffic, nil
}

func (s *Service) TrafficCheques() ([]*TrafficCheque, error) {
	s.trafficPeers.trafficLock.Lock()
	defer s.trafficPeers.trafficLock.Unlock()
	var trafficCheques []*TrafficCheque
	for chainAddress, traffic := range s.trafficPeers.trafficPeers {
		peer, known := s.addressBook.BeneficiaryPeer(common.HexToAddress(chainAddress))
		if known {
			trans := new(big.Int).Sub(traffic.transferTraffic, traffic.transferChequeTraffic)
			retrieve := new(big.Int).Sub(traffic.retrieveTraffic, traffic.retrieveChequeTraffic)
			trafficCheque := &TrafficCheque{
				Peer:                peer,
				OutstandingTraffic:  new(big.Int).Sub(trans, retrieve),
				SentSettlements:     traffic.retrieveChequeTraffic,
				ReceivedSettlements: traffic.transferChequeTraffic,
				Total:               new(big.Int).Sub(traffic.transferTraffic, traffic.retrieveTraffic),
				Uncashed:            new(big.Int).Sub(traffic.transferChequeTraffic, traffic.transferChainTraffic),
				Status:              traffic.status,
			}
			if trafficCheque.OutstandingTraffic.Cmp(big.NewInt(0)) == 0 && trafficCheque.SentSettlements.Cmp(big.NewInt(0)) == 0 && trafficCheque.ReceivedSettlements.Cmp(big.NewInt(0)) == 0 {
				continue
			}
			trafficCheques = append(trafficCheques, trafficCheque)
		} else {
			s.logger.Errorf("traffic: The method TrafficCheques failed to ChainAddress has no corresponding peer address.")
		}
	}

	return trafficCheques, nil
}

func (s *Service) Pay(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error {

	recipient, known := s.addressBook.Beneficiary(peer)
	if !known {
		s.logger.Warningf("disconnecting non-traffic peer %v", peer)
		err := s.p2pService.Disconnect(peer, "no recipient found")
		if err != nil {
			return err
		}
		return ErrUnknownBeneficary
	}
	balance := s.retrieveTraffic(recipient)
	traffic := s.getTraffic(recipient)
	traffic.Lock()
	defer traffic.Unlock()
	if balance.Cmp(paymentThreshold) >= 0 {
		if err := s.issue(ctx, peer, recipient, s.chainAddress, balance, traffic); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) issue(ctx context.Context, peer boson.Address, recipient, beneficiary common.Address, balance *big.Int, traffic *Traffic) error {

	defer func() {
		_ = s.notifyPaymentFunc(peer, balance)
	}()

	available, err := s.AvailableBalance()
	if err != nil {
		return err
	}

	if available.Cmp(balance) < 0 {
		return ErrInsufficientFunds
	}

	cumulativePayout := traffic.retrieveChequeTraffic
	// increase cumulativePayout by amount
	cumulativePayout = cumulativePayout.Add(cumulativePayout, balance)
	// create and sign the new cheque
	c := chequePkg.Cheque{
		Recipient:        recipient,
		Beneficiary:      beneficiary,
		CumulativePayout: cumulativePayout,
	}
	sin, err := s.chequeSigner.Sign(&c)
	if err != nil {
		return err
	}
	signedCheque := &chequePkg.SignedCheque{
		Cheque:    c,
		Signature: sin,
	}
	if err := s.protocol.EmitCheque(ctx, peer, signedCheque); err != nil {
		return err
	}
	return s.putSendCheque(ctx, &c, recipient, traffic)
}

func (s *Service) putSendCheque(ctx context.Context, cheque *chequePkg.Cheque, recipient common.Address, traffic *Traffic) error {
	traffic.retrieveChequeTraffic = cheque.CumulativePayout
	traffic.retrieveTraffic = s.maxBigint(traffic.retrieveTraffic, cheque.CumulativePayout)
	go s.PublishTrafficCheque(recipient)
	return s.chequeStore.PutSendCheque(ctx, cheque, recipient)
}

// TotalSent returns the total amount sent to a peer
func (s *Service) TotalSent(peer boson.Address) (totalSent *big.Int, err error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)
	totalSent = big.NewInt(0)
	if !known {
		return totalSent, chequePkg.ErrNoCheque
	}

	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	totalSent = traffic.transferTraffic
	return
}

// TotalReceived returns the total amount received from a peer
func (s *Service) TotalReceived(peer boson.Address) (totalReceived *big.Int, err error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)
	totalReceived = big.NewInt(0)

	if !known {
		return totalReceived, chequePkg.ErrNoCheque
	}

	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	totalReceived = traffic.retrieveTraffic
	return
}

func (s *Service) TransferTraffic(peer boson.Address) (*big.Int, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return big.NewInt(0), chequePkg.ErrNoCheque
	}

	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	return new(big.Int).Sub(traffic.transferTraffic, traffic.transferChequeTraffic), nil
}

func (s *Service) RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return big.NewInt(0), chequePkg.ErrNoCheque
	}
	return s.retrieveTraffic(chainAddress), nil
}

func (s *Service) retrieveTraffic(peer common.Address) *big.Int {
	traffic := s.getTraffic(peer)
	traffic.Lock()
	defer traffic.Unlock()
	return new(big.Int).Sub(traffic.retrieveTraffic, traffic.retrieveChequeTraffic)
}

func (s *Service) PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error {
	chainAddress, known := s.addressBook.Beneficiary(peer)
	if !known {
		return chequePkg.ErrNoCheque
	}

	chainTraffic := s.getTraffic(chainAddress)
	chainTraffic.Lock()
	chainTraffic.retrieveTraffic = new(big.Int).Add(chainTraffic.retrieveTraffic, traffic)
	chainTraffic.Unlock()
	go s.PublishHeader()
	return s.chequeStore.PutRetrieveTraffic(chainAddress, chainTraffic.retrieveTraffic)
}

func (s *Service) PutTransferTraffic(peer boson.Address, traffic *big.Int) error {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return chequePkg.ErrNoCheque
	}
	localTraffic := s.getTraffic(chainAddress)
	localTraffic.Lock()
	localTraffic.transferTraffic = new(big.Int).Add(localTraffic.transferTraffic, traffic)
	localTraffic.Unlock()
	go s.PublishHeader()
	return s.chequeStore.PutTransferTraffic(chainAddress, localTraffic.transferTraffic)
}

// AvailableBalance Get actual available balance
func (s *Service) AvailableBalance() (*big.Int, error) {
	s.trafficPeers.trafficLock.Lock()
	defer s.trafficPeers.trafficLock.Unlock()
	cashed := big.NewInt(0)
	transfer := big.NewInt(0)
	for _, traffic := range s.trafficPeers.trafficPeers {
		cashed = new(big.Int).Add(cashed, traffic.retrieveChainTraffic)
		transfer = new(big.Int).Add(transfer, traffic.retrieveTraffic)
	}

	return new(big.Int).Add(s.trafficPeers.balance, new(big.Int).Sub(cashed, transfer)), nil
}

func (s *Service) Handshake(peer boson.Address, recipient common.Address, signedCheque *chequePkg.SignedCheque) error {
	recipientLocal, known := s.addressBook.Beneficiary(peer)

	if !known {
		s.logger.Tracef("initial swap handshake peer: %v recipient: %x", peer, recipient)
		_, known := s.addressBook.BeneficiaryPeer(recipient)
		if known {
			return fmt.Errorf("overlay is exists")
		}
		if err := s.addressBook.PutBeneficiary(peer, recipient); err != nil {
			return err
		}
	} else {
		if signedCheque.Signature != nil && signedCheque.Recipient != recipientLocal {
			return fmt.Errorf("error in verifying check receiver ")
		}
	}

	err := s.UpdatePeerBalance(peer)
	if err != nil {
		return err
	}

	if signedCheque == nil || signedCheque.Signature == nil {
		return nil
	}

	isUser, err := s.chequeStore.VerifyCheque(signedCheque, s.chainID) //chequePkg.RecoverCheque(cheque, s.chainID)
	if err != nil {
		return err
	}
	if isUser != s.chainAddress {
		return chequePkg.ErrChequeInvalid
	}

	cheque, err := s.chequeStore.LastSendCheque(recipient)
	if err != nil && err != chequePkg.ErrNoCheque {
		return err
	}
	if err == chequePkg.ErrNoCheque {
		cheque = &chequePkg.Cheque{
			CumulativePayout: new(big.Int).SetInt64(0),
		}

	}

	if signedCheque.CumulativePayout.Cmp(cheque.CumulativePayout) > 0 {
		traffic := s.getTraffic(recipient)
		return s.putSendCheque(context.Background(), &signedCheque.Cheque, recipient, traffic)
	}

	return nil
}

func (s *Service) UpdatePeerBalance(peer boson.Address) error {
	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return chequePkg.ErrNoCheque
	}

	balance, err := s.trafficChainService.BalanceOf(chainAddress)
	if err != nil {
		return err
	}
	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	traffic.trafficPeerBalance = balance
	return nil
}

func (s *Service) ReceiveCheque(ctx context.Context, peer boson.Address, cheque *chequePkg.SignedCheque) error {

	chainAddress, known := s.addressBook.Beneficiary(peer)

	if !known {
		return errors.New("account information error")
	}

	if cheque.Beneficiary != chainAddress && cheque.Recipient != s.Address() {
		return errors.New("account information error ")
	}

	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	_, err := s.chequeStore.ReceiveCheque(ctx, cheque)
	if err != nil {
		return err
	}
	traffic.transferChequeTraffic = cheque.CumulativePayout
	go s.PublishTrafficCheque(chainAddress)
	return nil
}

func (s *Service) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.notifyPaymentFunc = notifyPaymentFunc
}

func (s *Service) GetPeerBalance(peer boson.Address) (*big.Int, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)
	if !known {
		return big.NewInt(0), chequePkg.ErrNoCheque
	}
	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	return traffic.trafficPeerBalance, nil
}

func (s *Service) GetUnPaidBalance(peer boson.Address) (*big.Int, error) {
	chainAddress, known := s.addressBook.Beneficiary(peer)
	if !known {
		return big.NewInt(0), chequePkg.ErrNoCheque
	}

	traffic := s.getTraffic(chainAddress)
	traffic.Lock()
	defer traffic.Unlock()
	transferTraffic := new(big.Int).Sub(traffic.transferTraffic, traffic.transferChainTraffic)
	return transferTraffic, nil
}

func (s *Service) TrafficInit() error {
	return s.Init()
}

func (s *Service) cashChequeReceiptUpdate() {

	go func() {
		tranReceipt := func(txHash common.Hash) (uint64, error) {
			status, err := s.cashout.WaitForReceipt(context.Background(), txHash)
			if err != nil {
				return 0, err
			}
			if status == 0 {
				s.logger.Errorf("traffic:cashChequeReceiptUpdate - %s Exchange failed ", txHash.String())
			}
			return status, nil
		}

		cashUpdate := func(beneficiary common.Address, peer boson.Address) error {
			balance, err := s.trafficChainService.BalanceOf(s.chainAddress)
			if err != nil {
				return fmt.Errorf("failed to get the chain balance")
			}
			s.peersLock.Lock()
			s.trafficPeers.balance = balance
			s.peersLock.Unlock()
			_ = s.getTraffic(beneficiary)
			err = s.trafficPeerChainUpdate(beneficiary, s.chainAddress)
			if err != nil {
				return err
			}
			return s.UpdatePeerBalance(peer)
		}

		for cashInfo := range s.cashChequeChan {
			status, err := tranReceipt(cashInfo.txHash)
			traffic := s.getTraffic(cashInfo.chainAddress)
			traffic.updateStatus(UnOperation)
			if err != nil {
				s.Publish(fmt.Sprintf("CashOut:%s", cashInfo.peer.String()),
					CashOutStatus{Overlay: cashInfo.peer, Status: false})
				continue
			}
			if status == 1 {
				err := cashUpdate(cashInfo.chainAddress, cashInfo.peer)
				if err != nil {
					s.logger.Errorf("traffic:cashChequeReceiptUpdate - %v ", err.Error())
					continue
				}
				go s.PublishHeader()
				go s.PublishTrafficCheque(cashInfo.chainAddress)
				s.Publish(fmt.Sprintf("CashOut:%s", cashInfo.peer.String()),
					CashOutStatus{Overlay: cashInfo.peer, Status: true})
			} else {
				s.Publish(fmt.Sprintf("CashOut:%s", cashInfo.peer.String()),
					CashOutStatus{Overlay: cashInfo.peer, Status: false})
			}
		}
	}()

}

func (s *Service) SubscribeHeader() (c <-chan interface{}, unsub func()) {
	channel := make(chan interface{}, 1)
	s.Subscribe("header", channel)
	unsub = func() {
		s.UnSubscribe("header", channel)
	}
	return channel, unsub
}

func (s *Service) SubscribeTrafficCheque(addresses []common.Address) (c <-chan interface{}, unsub func()) {
	channel := make(chan interface{}, len(addresses))
	for _, address := range addresses {
		s.Subscribe(fmt.Sprintf("TrafficCheque:%s", address.String()), channel)
	}

	unsub = func() {
		for _, address := range addresses {
			s.UnSubscribe(fmt.Sprintf("TrafficCheque:%s", address.String()), channel)
		}
	}
	return channel, unsub
}

func (s *Service) SubscribeCashOut(peers []boson.Address) (c <-chan interface{}, unsub func()) {
	channel := make(chan interface{}, len(peers))
	for _, overlay := range peers {
		s.Subscribe(fmt.Sprintf("CashOut:%s", overlay.String()), channel)
	}

	unsub = func() {
		for _, overlay := range peers {
			s.UnSubscribe(fmt.Sprintf("CashOut:%s", overlay.String()), channel)
		}
	}
	return channel, unsub
}

func (s *Service) Subscribe(key string, c chan interface{}) {
	s.pubSubLk.Lock()
	defer s.pubSubLk.Unlock()
	if _, ok := s.pubSub[key]; !ok {
		ch := []chan interface{}{c}
		s.pubSub[key] = ch
	} else {
		s.pubSub[key] = append(s.pubSub[key], c)
	}

}

func (s *Service) UnSubscribe(key string, ch chan interface{}) {
	s.pubSubLk.Lock()
	defer s.pubSubLk.Unlock()
	defer func() {
		recover()
	}()
	if len(s.pubSub[key]) == 1 {
		delete(s.pubSub, key)
		close(ch)
	} else {
		for i, c := range s.pubSub[key] {
			if c == ch {
				s.pubSub[key] = append(s.pubSub[key][:i], s.pubSub[key][i+1:]...)
				close(ch)
			}
		}
	}
}

func (s *Service) Publish(key string, data interface{}) {
	s.pubSubLk.RLock()
	defer s.pubSubLk.RUnlock()
	defer func() {
		recover()
	}()
	if c, ok := s.pubSub[key]; ok {
		for _, i := range c {
			i <- data
		}
	}
}

func (s *Service) PublishHeader() {
	trafficInfo, _ := s.TrafficInfo()
	s.Publish("header", &trafficInfo)
}

func (s *Service) PublishTrafficCheque(overlay common.Address) {
	traffic := s.getTraffic(overlay)
	traffic.Lock()
	trans := new(big.Int).Sub(traffic.transferTraffic, traffic.transferChequeTraffic)
	retrieve := new(big.Int).Sub(traffic.retrieveTraffic, traffic.retrieveChequeTraffic)
	peer, _ := s.addressBook.BeneficiaryPeer(overlay)
	trafficCheque := &TrafficCheque{
		Peer:                peer,
		OutstandingTraffic:  new(big.Int).Sub(trans, retrieve),
		SentSettlements:     traffic.retrieveChequeTraffic,
		ReceivedSettlements: traffic.transferChequeTraffic,
		Total:               new(big.Int).Sub(traffic.transferTraffic, traffic.retrieveTraffic),
		Uncashed:            new(big.Int).Sub(traffic.transferChequeTraffic, traffic.transferChainTraffic),
		Status:              traffic.status,
	}
	traffic.Unlock()
	s.Publish(fmt.Sprintf("TrafficCheque:%s", overlay.String()), *trafficCheque)
}

func (t *Traffic) updateStatus(status CashStatus) {
	t.Lock()
	defer t.Unlock()
	t.status = status
}
