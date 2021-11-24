package traffic

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
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

type Interface interface {
	// LastSentCheque returns the last sent cheque for the peer
	LastSentCheque(peer common.Address) (*cheque.SignedCheque, error)
	// LastReceivedCheques returns the list of last received cheques for all peers
	LastReceivedCheques() (map[common.Address]*cheque.SignedCheque, error)
	// CashCheque sends a cashing transaction for the last cheque of the peer
	CashCheque(ctx context.Context, peer common.Address) (common.Hash, error)
}

type Service struct {
	logger              logging.Logger
	chainAddress        common.Address
	store               storage.StateStorer
	notifyPaymentFunc   settlement.NotifyPaymentFunc
	metrics             metrics
	chequeStore         cheque.ChequeStore
	cheque              cheque.Cheque
	cashout             cheque.CashoutService
	trafficChainService chain.Traffic
	transactionService  transaction.Service
	p2pService          p2p.Service
	trafficPeers        TrafficPeer
}

func New(logger logging.Logger, chainAddress common.Address, store storage.StateStorer, chequeStore cheque.ChequeStore, cheque cheque.Cheque, cashout cheque.CashoutService, p2pService p2p.Service) *Service {
	return &Service{
		logger:       logger,
		store:        store,
		chainAddress: chainAddress,
		metrics:      newMetrics(),
		chequeStore:  chequeStore,
		cashout:      cashout,
		cheque:       cheque,
		p2pService:   p2pService,
	}
}

func (s *Service) InitChain() error {
	var chainData map[common.Address]Traffic

	s.trafficPeers = TrafficPeer{}

	lastCheques, err := s.chequeStore.LastSendCheques() //从本地拿最后各节点最后一张获流支票
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
			traffic.retrieveChequeTraffic = s.maxBigint(v.retrieveChequeTraffic, cq.CumulativePayout) //修改获流值
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
func (s *Service) LastSentCheque(peer common.Address) (*cheque.SignedCheque, error) {
	return s.chequeStore.LastSentCheque(peer)
}

// LastReceivedCheques returns the list of last received cheques for all peers
func (s *Service) LastReceivedCheques() (map[common.Address]*cheque.SignedCheque, error) {
	return s.chequeStore.LastCheques()
}

// CashCheque sends a cashing transaction for the last cheque of the peer
func (s *Service) CashCheque(ctx context.Context, peer common.Address) (common.Hash, error) {
	return nil, nil
}
