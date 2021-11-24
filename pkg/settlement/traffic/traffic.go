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
	LastSentCheque(peer boson.Address) (*cheque.SignedCheque, error)
	// LastReceivedCheques returns the list of last received cheques for all peers
	LastReceivedCheques() (map[string]*cheque.SignedCheque, error)
	// CashCheque sends a cashing transaction for the last cheque of the peer
	CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error)
}

type Service struct {
	logger              logging.Logger
	store               storage.StateStorer
	notifyPaymentFunc   settlement.NotifyPaymentFunc
	metrics             metrics
	chequeStore         cheque.ChequeStore
	cheque              cheque.Cheque
	cashout             cheque.CashoutService
	trafficChainService chain.Traffic
	transactionService  transaction.Service
	p2pService          p2p.Service
}

func New(logger logging.Logger, store storage.StateStorer, chequeStore cheque.ChequeStore, cheque cheque.Cheque, cashout cheque.CashoutService, p2pService p2p.Service) *Service {
	return &Service{
		logger:      logger,
		store:       store,
		metrics:     newMetrics(),
		chequeStore: chequeStore,
		cashout:     cashout,
		cheque:      cheque,
		p2pService:  p2pService,
	}
}
