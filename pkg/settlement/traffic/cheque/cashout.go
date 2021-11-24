package cheque

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// CashoutService is the service responsible for managing cashout actions
type CashoutService interface {
	// CashCheque
	CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error)
}

type cashoutService struct {
	store              storage.StateStorer
	transactionService chain.Transaction
	chequeStore        ChequeStore
}

// NewCashoutService creates a new CashoutService
func NewCashoutService(
	store storage.StateStorer,
	transactionService chain.Transaction,
	chequeStore ChequeStore,
) CashoutService {
	return &cashoutService{
		store:              store,
		transactionService: transactionService,
		chequeStore:        chequeStore,
	}
}

// CashCheque
func (s *cashoutService) CashCheque(ctx context.Context, chequebook, recipient common.Address) (common.Hash, error) {
	cheque, err := s.chequeStore.LastReceivedCheque(chequebook)
	if err != nil {
		return common.Hash{}, err
	}

	callData, err := trafficAbi.Pack("cashChequeBeneficiary", recipient, cheque.CumulativePayout, cheque.Signature)
	if err != nil {
		return common.Hash{}, err
	}
	lim := sctx.GetGasLimit(ctx)
	if lim == 0 {
		// fix for out of gas errors
		lim = 300000
	}
	request := &transaction.TxRequest{
		To:       &chequebook,
		Data:     callData,
		GasPrice: sctx.GetGasPrice(ctx),
		GasLimit: lim,
		Value:    big.NewInt(0),
	}

	txHash, err := s.transactionService.Send(ctx, request)
	if err != nil {
		return common.Hash{}, err
	}

	return txHash, nil
}
