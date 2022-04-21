package cheque

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// CashoutService is the service responsible for managing cashout actions
type CashoutService interface {
	// CashCheque
	CashCheque(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address) (common.Hash, error)
	//WaitForReceipt
	WaitForReceipt(ctx context.Context, ctxHash common.Hash) (uint64, error)
}

type cashoutService struct {
	store               storage.StateStorer
	transactionService  chain.Transaction
	trafficService      chain.Traffic
	chequeStore         ChequeStore
	trafficContractAddr common.Address
}

// NewCashoutService creates a new CashoutService
func NewCashoutService(store storage.StateStorer, transactionService chain.Transaction, trafficService chain.Traffic, chequeStore ChequeStore, trafficContractAddr common.Address) CashoutService {

	return &cashoutService{
		store:               store,
		transactionService:  transactionService,
		trafficService:      trafficService,
		chequeStore:         chequeStore,
		trafficContractAddr: trafficContractAddr,
	}
}

// CashCheque
func (s *cashoutService) CashCheque(ctx context.Context, peer boson.Address, beneficiary, recipient common.Address) (common.Hash, error) {
	cheque, err := s.chequeStore.LastReceivedCheque(beneficiary)
	if err != nil {
		return common.Hash{}, err
	}

	if beneficiary != cheque.Beneficiary {
		return common.Hash{}, errors.New("exchange failed")
	}

	tx, err := s.trafficService.CashChequeBeneficiary(ctx, peer, beneficiary, recipient, cheque.CumulativePayout, cheque.Signature)
	if err != nil {
		return common.Hash{}, err
	}

	if err != nil {
		return common.Hash{}, err
	}

	return tx.Hash(), nil
}

func (s *cashoutService) WaitForReceipt(ctx context.Context, ctxHash common.Hash) (uint64, error) {
	receipt, err := s.transactionService.WaitForReceipt(ctx, ctxHash)

	return receipt.Status, err

}
