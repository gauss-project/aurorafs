package cheque

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// CashoutService is the service responsible for managing cashout actions
type CashoutService interface {
	// CashCheque
	CashCheque(ctx context.Context, beneficiary common.Address, recipient common.Address) (common.Hash, error)
}

type cashoutService struct {
	store               storage.StateStorer
	transactionService  chain.Transaction
	cashTrafficService  chain.Traffic
	chequeStore         ChequeStore
	trafficContractAddr common.Address
}

// NewCashoutService creates a new CashoutService
func NewCashoutService(store storage.StateStorer, transactionService chain.Transaction, chequeStore ChequeStore, trafficContractAddr common.Address) CashoutService {
	return &cashoutService{
		store:               store,
		transactionService:  transactionService,
		chequeStore:         chequeStore,
		trafficContractAddr: trafficContractAddr,
	}
}

// CashCheque
func (s *cashoutService) CashCheque(ctx context.Context, beneficiary, recipient common.Address) (common.Hash, error) {
	cheque, err := s.chequeStore.LastReceivedCheque(beneficiary)
	if err != nil {
		return common.Hash{}, err
	}

	if beneficiary != cheque.Beneficiary {
		return common.Hash{}, errors.New("exchange failed")
	}

	txData, err := s.cashTrafficService.CashChequeBeneficiary(beneficiary, recipient, cheque.CumulativePayout, cheque.Signature)
	if err != nil {
		return common.Hash{}, err
	}

	txHash, err := s.transactionService.Send(ctx, txData)
	if err != nil {
		return common.Hash{}, err
	}

	return txHash, nil
}
