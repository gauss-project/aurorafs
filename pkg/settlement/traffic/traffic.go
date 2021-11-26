package traffic

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/storage"

	"math/big"
	"sync"
)

var (
	ErrUnknownBeneficary = errors.New("unknown beneficiary for peer")
	ErrInsufficientFunds = errors.New("insufficient token balance")
)

type SendChequeFunc func(cheque *chequePkg.SignedCheque) error

type Traffic struct {
	trafficPeerBalance    *big.Int
	retrieveChainTraffic  *big.Int
	transferChainTraffic  *big.Int
	retrieveChequeTraffic *big.Int
	transferChequeTraffic *big.Int
	retrieveTraffic       *big.Int
	transferTraffic       *big.Int
}

type TrafficPeer struct {
	trafficPeers sync.Map
	balance      *big.Int
	totalPaidOut *big.Int
}

type TrafficCheque struct {
	Peer               boson.Address
	OutstandingTraffic *big.Int
	SendTraffic        *big.Int
	ReceivedTraffic    *big.Int
	Total              *big.Int
	Uncashed           *big.Int
}

type TrafficInfo struct {
	Balance          *big.Int
	AvailableBalance *big.Int
	TotalSendTraffic *big.Int
	ReceivedTraffic  *big.Int
}

type Interface interface {
	// LastSentCheque returns the last sent cheque for the peer
	LastSentCheque(peer boson.Address) (*chequePkg.Cheque, error)
	// LastReceivedCheques returns the list of last received cheques for all peers
	LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error)
	// CashCheque sends a cashing transaction for the last cheque of the peer
	CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error)

	TrafficCheques() ([]*TrafficCheque, error)

	Address() common.Address

	TrafficInfo() (*TrafficInfo, error)
}

type Service struct {
	logger              logging.Logger
	chainAddress        common.Address
	store               storage.StateStorer
	metrics             metrics
	chequeStore         chequePkg.ChequeStore
	cashout             chequePkg.CashoutService
	trafficChainService chain.Traffic
	p2pService          p2p.Service
	trafficPeers        TrafficPeer
	addressBook         Addressbook
	chequeSigner        chequePkg.ChequeSigner
	protocol            trafficprotocol.Interface
	notifyPaymentFunc   settlement.NotifyPaymentFunc
	chainID             int64
}

func New(logger logging.Logger, chainAddress common.Address, store storage.StateStorer, trafficChainService chain.Traffic,
	chequeStore chequePkg.ChequeStore, cashout chequePkg.CashoutService, p2pService p2p.Service, addressBook Addressbook,
	chequeSigner chequePkg.ChequeSigner, protocol trafficprotocol.Interface, chainID int64) *Service {
	return &Service{
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
	}
}

func (s *Service) InitTraffic() error {
	s.trafficPeers = TrafficPeer{}

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

	return s.addressBook.InitAddressBook()
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

	for k, _ := range lastCheques {
		if _, ok := chanResp[k]; !ok {
			chanResp[k] = Traffic{}
		}
	}

	for k, _ := range lastTransCheques {
		if _, ok := chanResp[k]; !ok {
			chanResp[k] = Traffic{}
		}
	}

	return chanResp, err
}

func (s *Service) replaceTraffic(addressList map[common.Address]Traffic, lastCheques map[common.Address]*chequePkg.Cheque, lastTransCheques map[common.Address]*chequePkg.SignedCheque) error {
	s.trafficPeers.totalPaidOut = new(big.Int).SetInt64(0)
	for k, _ := range addressList {
		retrievedTotal, err := s.trafficChainService.TransAmount(k, s.chainAddress)
		if err != nil {
			return nil
		}
		transferTotal, err := s.trafficChainService.TransAmount(s.chainAddress, k)
		if err != nil {
			return nil
		}

		traffic := Traffic{
			retrieveChainTraffic:  retrievedTotal,
			transferChainTraffic:  transferTotal,
			transferChequeTraffic: transferTotal,
			transferTraffic:       transferTotal,
			retrieveChequeTraffic: retrievedTotal,
			retrieveTraffic:       retrievedTotal,
		}
		if cq, ok := lastCheques[k]; !ok {
			traffic.retrieveTraffic = s.maxBigint(traffic.retrieveTraffic, cq.CumulativePayout)
			traffic.retrieveChequeTraffic = s.maxBigint(traffic.retrieveChequeTraffic, cq.CumulativePayout)
		}

		if cq, ok := lastTransCheques[k]; !ok {
			traffic.transferTraffic = s.maxBigint(traffic.transferTraffic, cq.CumulativePayout)
			traffic.transferChequeTraffic = s.maxBigint(traffic.retrieveChequeTraffic, cq.CumulativePayout)
		}

		s.trafficPeers.trafficPeers.Store(k, traffic)
	}
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
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return nil, err
	}
	if !known {
		return nil, chequePkg.ErrNoCheque
	}
	return s.chequeStore.LastSendCheque(chainAddress)
}

// LastReceivedCheque returns the list of last received cheques for all peers
func (s *Service) LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return nil, err
	}
	if !known {
		return nil, chequePkg.ErrNoCheque
	}
	return s.chequeStore.LastReceivedCheque(chainAddress)
}

// CashCheque sends a cashing transaction for the last cheque of the peer
func (s *Service) CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return common.Hash{}, err
	}
	if !known {
		return common.Hash{}, chequePkg.ErrNoCheque
	}
	sign, err := s.chequeStore.LastReceivedCheque(chainAddress)

	if err != nil {
		return common.Hash{}, err
	}

	if chainAddress != sign.Recipient {
		return common.Hash{}, errors.New("exchange failed")
	}

	c, err := s.cashout.CashCheque(ctx, chainAddress, sign.Recipient)
	if err != nil {
		return common.Hash{}, err
	}
	return c, err
}

func (s *Service) Address() common.Address {
	return s.chainAddress
}

func (s *Service) TrafficInfo() (*TrafficInfo, error) {
	var respTraffic TrafficInfo
	cashed := big.NewInt(0)
	transfer := big.NewInt(0)
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		traffic := v.(Traffic)
		cashed = new(big.Int).Add(cashed, traffic.transferChainTraffic)
		transfer = new(big.Int).Add(transfer, traffic.transferTraffic)
		respTraffic.TotalSendTraffic = new(big.Int).Add(respTraffic.TotalSendTraffic, traffic.retrieveChequeTraffic)
		respTraffic.ReceivedTraffic = new(big.Int).Add(respTraffic.ReceivedTraffic, traffic.transferChequeTraffic)
		return true
	})

	respTraffic.Balance = s.trafficPeers.balance
	respTraffic.AvailableBalance = new(big.Int).Add(respTraffic.Balance, new(big.Int).Sub(cashed, transfer))

	return &respTraffic, nil
}

func (s *Service) TrafficCheques() ([]*TrafficCheque, error) {
	var trafficCheques []*TrafficCheque
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {

		peer, known, err := s.addressBook.BeneficiaryPeer(chainAddress.(common.Address))
		if err == nil && known {
			traffic := v.(Traffic)
			trafficCheque := &TrafficCheque{
				Peer:               peer,
				OutstandingTraffic: new(big.Int).Sub(traffic.transferChequeTraffic, traffic.retrieveChequeTraffic),
				SendTraffic:        traffic.retrieveChequeTraffic,
				ReceivedTraffic:    traffic.transferChequeTraffic,
				Total:              new(big.Int).Add(traffic.retrieveTraffic, traffic.transferTraffic),
				Uncashed:           new(big.Int).Sub(traffic.transferChequeTraffic, traffic.transferChainTraffic),
			}
			trafficCheques = append(trafficCheques, trafficCheque)
		} else {
			if !known {
				s.logger.Errorf("traffic: The method TrafficCheques failed to ChainAddress has no corresponding peer address.")
			} else {
				s.logger.Errorf("traffic: The method TrafficCheques failed to get the peer address,%v", err.Error())
			}

		}
		return true
	})

	return trafficCheques, nil
}

func (s *Service) Pay(ctx context.Context, peer boson.Address, traffic, paymentThreshold *big.Int) error {
	recipient, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return err
	}
	if !known {
		s.logger.Warningf("disconnecting non-traffic peer %v", peer)
		err = s.p2pService.Disconnect(peer, "no recipient found")
		if err != nil {
			return err
		}
		return ErrUnknownBeneficary
	}
	balance, err := s.RetrieveTraffic(peer)
	balance = balance.Add(balance, traffic)
	if balance.Cmp(paymentThreshold) >= 0 {
		if err := s.Issue(ctx, peer, recipient, s.chainAddress, traffic); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) Issue(ctx context.Context, peer boson.Address, recipient, beneficiary common.Address, traffic *big.Int) error {
	available, err := s.AvailableBalance(ctx)
	if err != nil {
		return err
	}

	if available.Cmp(traffic) < 0 {
		return ErrInsufficientFunds
	}

	var cumulativePayout *big.Int
	lastCheque, err := s.LastSentCheque(peer)
	if err != nil {
		if err != chequePkg.ErrNoCheque {
			return err
		}
		cumulativePayout = big.NewInt(0)
	} else {
		cumulativePayout = lastCheque.CumulativePayout
	}
	// increase cumulativePayout by amount
	cumulativePayout = cumulativePayout.Add(cumulativePayout, traffic)
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
	if err := s.chequeStore.PutSendCheque(ctx, &c, recipient); err != nil {
		return err
	}
	return s.notifyPaymentFunc(peer, traffic)
}

// TotalSent returns the total amount sent to a peer
func (s *Service) TotalSent(peer boson.Address) (totalSent *big.Int, err error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	totalSent = big.NewInt(0)
	if err != nil {
		return totalSent, err
	}
	if !known {
		return totalSent, chequePkg.ErrNoCheque
	}

	if v, ok := s.trafficPeers.trafficPeers.Load(chainAddress.String()); !ok {
		totalSent = v.(Traffic).transferTraffic
	}
	return totalSent, nil
}

// TotalReceived returns the total amount received from a peer
func (s *Service) TotalReceived(peer boson.Address) (totalReceived *big.Int, err error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	totalReceived = new(big.Int).SetInt64(0)
	if err != nil {
		return totalReceived, err
	}
	if !known {
		return totalReceived, chequePkg.ErrNoCheque
	}

	if v, ok := s.trafficPeers.trafficPeers.Load(chainAddress.String()); !ok {
		totalReceived = v.(Traffic).retrieveTraffic
	}
	return totalReceived, nil
}

// SettlementsSent returns sent settlements for each individual known peer
func (s *Service) SettlementsSent() (map[string]*big.Int, error) {
	respSent := make(map[string]*big.Int)
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		if _, ok := respSent[chainAddress.(common.Address).String()]; !ok {
			respSent[chainAddress.(common.Address).String()] = v.(Traffic).retrieveChequeTraffic
		}
		return true
	})
	return respSent, nil
}

// SettlementsReceived returns received settlements for each individual known peer
func (s *Service) SettlementsReceived() (map[string]*big.Int, error) {
	respReceived := make(map[string]*big.Int)
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		if _, ok := respReceived[chainAddress.(common.Address).String()]; !ok {
			respReceived[chainAddress.(common.Address).String()] = v.(Traffic).transferChequeTraffic
		}
		return true
	})
	return respReceived, nil
}

func (s *Service) TransferTraffic(peer boson.Address) (traffic *big.Int, err error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return new(big.Int).SetInt64(0), err
	}
	if !known {
		return new(big.Int).SetInt64(0), chequePkg.ErrNoCheque
	}
	var transferTraffic, transferChequeTraffic *big.Int
	tra, ok := s.trafficPeers.trafficPeers.Load(chainAddress)

	if ok {
		transferTraffic = tra.(Traffic).transferTraffic
		transferChequeTraffic = tra.(Traffic).transferChequeTraffic
	} else {
		transferTraffic = big.NewInt(0)
		transferChequeTraffic = big.NewInt(0)
	}

	return new(big.Int).Sub(transferTraffic, transferChequeTraffic), nil
}
func (s *Service) RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error) {
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return new(big.Int).SetInt64(0), err
	}
	if !known {
		return new(big.Int).SetInt64(0), chequePkg.ErrNoCheque
	}
	var retrieveTraffic, retrieveChequeTraffic *big.Int
	tra, ok := s.trafficPeers.trafficPeers.Load(chainAddress)

	if ok {
		retrieveTraffic = tra.(Traffic).retrieveTraffic
		retrieveChequeTraffic = tra.(Traffic).retrieveChequeTraffic
	} else {
		return big.NewInt(0), nil
	}

	return new(big.Int).Sub(retrieveTraffic, retrieveChequeTraffic), nil
}

func (s *Service) PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error {
	var localTraffic Traffic
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return err
	}
	if !known {
		return chequePkg.ErrNoCheque
	}
	chainTraffic, ok := s.trafficPeers.trafficPeers.Load(chainAddress.String())
	if !ok {
		localTraffic.retrieveTraffic = traffic
	} else {
		localTraffic = chainTraffic.(Traffic)
		localTraffic.retrieveTraffic = new(big.Int).Add(chainTraffic.(Traffic).retrieveTraffic, traffic)
	}
	s.trafficPeers.trafficPeers.Store(chainAddress.String(), localTraffic)
	return s.chequeStore.PutRetrieveTraffic(chainAddress, traffic)
}

func (s *Service) PutTransferTraffic(peer boson.Address, traffic *big.Int) error {
	var localTraffic Traffic
	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return err
	}
	if !known {
		return chequePkg.ErrNoCheque
	}
	chainTraffic, ok := s.trafficPeers.trafficPeers.Load(chainAddress.String())
	if !ok {
		localTraffic.transferTraffic = traffic
	} else {
		localTraffic = chainTraffic.(Traffic)
		localTraffic.transferTraffic = new(big.Int).Add(chainTraffic.(Traffic).transferTraffic, traffic)
	}
	s.trafficPeers.trafficPeers.Store(chainAddress.String(), localTraffic)

	return s.chequeStore.PutTransferTraffic(chainAddress, traffic)
}

// Balance Get chain balance
func (s *Service) Balance(ctx context.Context) (*big.Int, error) {
	return s.trafficPeers.balance, nil
}

// AvailableBalance Get actual available balance
func (s *Service) AvailableBalance(ctx context.Context) (*big.Int, error) {
	cashed := big.NewInt(0)
	transfer := big.NewInt(0)
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		traffic := v.(Traffic)
		cashed = new(big.Int).Add(cashed, traffic.transferChainTraffic)
		transfer = new(big.Int).Add(transfer, traffic.transferTraffic)
		return true
	})

	return new(big.Int).Add(s.trafficPeers.balance, new(big.Int).Sub(cashed, transfer)), nil
}

func (s *Service) Handshake(peer boson.Address, beneficiary common.Address, cheque *chequePkg.SignedCheque) error {
	issuser, err := chequePkg.RecoverCheque(cheque, s.chainID)
	if err != nil {
		return err
	}
	if issuser != s.chainAddress {
		return chequePkg.ErrChequeInvalid
	}

	singCheque, err := s.chequeStore.LastReceivedCheque(beneficiary)
	if err != nil {
		return err
	}
	storedBeneficiary, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return err
	}
	if !known {
		s.logger.Tracef("initial swap handshake peer: %v beneficiary: %x", peer, beneficiary)
		return s.addressBook.PutBeneficiary(peer, beneficiary)
	}
	if storedBeneficiary != beneficiary {
		return fmt.Errorf("wrong beneficiary")
	}

	if cheque.CumulativePayout.Cmp(singCheque.CumulativePayout) > 0 {
		return s.chequeStore.PutSendCheque(context.Background(), &cheque.Cheque, beneficiary)
	}

	return nil
}

func (s *Service) UpdatePeerBalance(peer boson.Address) error {

	chainAddress, known, err := s.addressBook.Beneficiary(peer)
	if err != nil {
		return err
	}
	if !known {
		return chequePkg.ErrNoCheque
	}

	balance, err := s.trafficChainService.BalanceOf(chainAddress)
	if err != nil {
		return err
	}

	var localTraffic Traffic
	chainTraffic, ok := s.trafficPeers.trafficPeers.Load(chainAddress)

	updateTraffice := chainTraffic.(Traffic)
	if ok {
		localTraffic.transferTraffic = updateTraffice.transferTraffic
		localTraffic.retrieveTraffic = updateTraffice.retrieveTraffic
		localTraffic.transferChequeTraffic = updateTraffice.transferChequeTraffic
		localTraffic.retrieveChainTraffic = updateTraffice.retrieveChainTraffic
		localTraffic.transferChainTraffic = updateTraffice.transferChainTraffic
		localTraffic.retrieveChequeTraffic = updateTraffice.retrieveChequeTraffic
	}
	localTraffic.trafficPeerBalance = balance

	s.trafficPeers.trafficPeers.Store(chainAddress, localTraffic)

	return nil
}

func (s *Service) ReceiveCheque(ctx context.Context, peer boson.Address, cheque *chequePkg.SignedCheque) error {

	if cheque.Beneficiary != s.Address() {
		return errors.New("account information error ")
	}

	chainTraffic, ok := s.trafficPeers.trafficPeers.Load(peer)

	uncollectedCheque := new(big.Int).Sub(cheque.CumulativePayout, chainTraffic.(Traffic).retrieveChainTraffic)

	if !ok || chainTraffic.(Traffic).trafficPeerBalance.Cmp(uncollectedCheque) == -1 {
		return errors.New("The recipient is unable to pay ")
	}

	_, err := s.chequeStore.ReceiveCheque(ctx, cheque)

	return err
}

func (s *Service) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.notifyPaymentFunc = notifyPaymentFunc
}
