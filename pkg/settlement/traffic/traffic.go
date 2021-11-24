package traffic

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/storage"

	"math/big"
	"sync"
)

type Traffic struct {
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
	LastSentCheque(chainAddress common.Address) (*cheque.Cheque, error)
	// LastReceivedCheques returns the list of last received cheques for all peers
	LastReceivedCheque(chainAddress common.Address) (*cheque.SignedCheque, error)
	// CashCheque sends a cashing transaction for the last cheque of the peer
	CashCheque(ctx context.Context, chainAddress common.Address) (common.Hash, error)

	TrafficCheques() ([]*TrafficCheque, error)

	Address() common.Address

	TrafficInfo() (*TrafficInfo, error)
}

type Service struct {
	logger              logging.Logger
	chainAddress        common.Address
	store               storage.StateStorer
	notifyPaymentFunc   settlement.NotifyPaymentFunc
	metrics             metrics
	chequeStore         cheque.ChequeStore
	cashout             cheque.CashoutService
	trafficChainService chain.Traffic
	p2pService          p2p.Service
	trafficPeers        TrafficPeer
}

func New(logger logging.Logger, chainAddress common.Address, store storage.StateStorer, trafficChainService chain.Traffic, chequeStore cheque.ChequeStore, cashout cheque.CashoutService, p2pService p2p.Service) *Service {
	return &Service{
		logger:              logger,
		store:               store,
		chainAddress:        chainAddress,
		trafficChainService: trafficChainService,
		metrics:             newMetrics(),
		chequeStore:         chequeStore,
		cashout:             cashout,
		p2pService:          p2pService,
	}
}

func (s *Service) InitChain() error {
	var chainData map[common.Address]Traffic

	s.trafficPeers = TrafficPeer{}

	lastCheques, err := s.chequeStore.LastSendCheques()
	if err != nil {
		s.logger.Errorf("Traffic failed to obtain local check information. ")
		return err
	}

	for k, v := range chainData {
		traffic := Traffic{
			retrieveChainTraffic:  v.retrieveChainTraffic,
			transferChainTraffic:  v.transferChainTraffic,
			transferChequeTraffic: v.transferChequeTraffic,
			transferTraffic:       v.transferTraffic,
			retrieveChequeTraffic: v.retrieveChequeTraffic,
			retrieveTraffic:       v.retrieveTraffic,
		}
		if cq, ok := lastCheques[k]; !ok {
			traffic.retrieveTraffic = s.maxBigint(v.retrieveTraffic, cq.CumulativePayout)
			traffic.retrieveChequeTraffic = s.maxBigint(v.retrieveChequeTraffic, cq.CumulativePayout)
		}

		s.trafficPeers.trafficPeers.Store(k, Traffic{})
	}
	s.trafficPeers.balance = new(big.Int).SetInt64(0)
	s.trafficPeers.totalPaidOut = new(big.Int).SetInt64(0)

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
func (s *Service) LastSentCheque(chainAddress common.Address) (*cheque.Cheque, error) {
	return s.chequeStore.LastSendCheque(chainAddress)
}

// LastReceivedCheques returns the list of last received cheques for all peers
func (s *Service) LastReceivedCheque(chainAddress common.Address) (*cheque.SignedCheque, error) {
	return s.chequeStore.LastReceivedCheque(chainAddress)
}

// CashCheque sends a cashing transaction for the last cheque of the peer
func (s *Service) CashCheque(ctx context.Context, chainAddress common.Address) (common.Hash, error) {
	return common.Hash{}, nil
}

func (s *Service) Address() common.Address {
	return common.Address{}
}

func (s *Service) TrafficInfo() (*TrafficInfo, error) {
	var respTraffic TrafficInfo
	cashed := big.NewInt(0)
	transfer := big.NewInt(0)
	totalSent := big.NewInt(0)
	totalReceived := big.NewInt(0)
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		traffic := v.(Traffic)
		cashed = new(big.Int).Add(cashed, traffic.transferChainTraffic)
		transfer = new(big.Int).Add(transfer, traffic.transferTraffic)
		totalSent = new(big.Int).Add(totalSent, traffic.retrieveChequeTraffic)
		totalReceived = new(big.Int).Add(totalReceived, traffic.transferChequeTraffic)
		return true
	})

	respTraffic.Balance = s.trafficPeers.balance
	respTraffic.AvailableBalance = new(big.Int).Add(respTraffic.Balance, new(big.Int).Sub(cashed, transfer))
	respTraffic.TotalSendTraffic = totalSent
	respTraffic.ReceivedTraffic = totalReceived

	return &respTraffic, nil
}

func (s *Service) TrafficCheques() ([]*TrafficCheque, error) {
	var trafficCheques []*TrafficCheque
	s.trafficPeers.trafficPeers.Range(func(chainAddress, v interface{}) bool {
		traffic := v.(Traffic)
		trafficCheque := &TrafficCheque{
			Peer:               boson.NewAddress(chainAddress.(common.Address).Bytes()),
			OutstandingTraffic: new(big.Int).Sub(traffic.transferChequeTraffic, traffic.retrieveChequeTraffic),
			SendTraffic:        traffic.retrieveChequeTraffic,
			ReceivedTraffic:    traffic.transferChequeTraffic,
			Total:              new(big.Int).Add(traffic.retrieveTraffic, traffic.transferTraffic),
			Uncashed:           new(big.Int).Sub(traffic.transferChequeTraffic, traffic.transferChainTraffic),
		}
		trafficCheques = append(trafficCheques, trafficCheque)
		return true
	})

	return trafficCheques, nil
}

func (s *Service) Pay(ctx context.Context, chainAddress common.Address, traffic *big.Int) error {
	return nil
}

// TotalSent returns the total amount sent to a peer
func (s *Service) TotalSent(chainAddress common.Address) (totalSent *big.Int, err error) {
	totalSent = new(big.Int).SetInt64(0)

	if v, ok := s.trafficPeers.trafficPeers.Load(chainAddress.String()); !ok {
		totalSent = v.(Traffic).transferTraffic
	}
	return totalSent, nil
}

// TotalReceived returns the total amount received from a peer
func (s *Service) TotalReceived(chainAddress common.Address) (totalReceived *big.Int, err error) {
	totalReceived = new(big.Int).SetInt64(0)

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

// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
func (s *Service) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.notifyPaymentFunc = notifyPaymentFunc
}

func (s *Service) TransferTraffic(chainAddress common.Address) (traffic *big.Int, err error) {
	return new(big.Int).SetInt64(0), nil
}
func (s *Service) RetrieveTraffic(chainAddress common.Address) (traffic *big.Int, err error) {
	return new(big.Int).SetInt64(0), nil
}

func (s *Service) PutRetrieveTraffic(chainAddress common.Address, traffic *big.Int) error {
	var localTraffic Traffic
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

func (s *Service) PutTransferTraffic(chainAddress common.Address, traffic *big.Int) error {
	var localTraffic Traffic
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
