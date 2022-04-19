package traffic

import (
	"context"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
	"time"
)

type ChainTraffic struct {
	logger             logging.Logger
	signer             crypto.Signer
	chainID            *big.Int
	backend            *ethclient.Client
	traffic            *Traffic
	transactionService chain.Transaction
}

func NewServer(logger logging.Logger, chainID *big.Int, backend *ethclient.Client, signer crypto.Signer, transactionService chain.Transaction, address string) (chain.Traffic, error) {

	traffic, err := NewTraffic(common.HexToAddress(address), backend)
	if err != nil {
		logger.Errorf("Failed to connect to the Ethereum client: %v", err)
		return &ChainTraffic{}, err
	}
	return &ChainTraffic{
		logger:             logger,
		signer:             signer,
		chainID:            chainID,
		traffic:            traffic,
		backend:            backend,
		transactionService: transactionService,
	}, nil
}

func (chainTraffic *ChainTraffic) TransferredAddress(address common.Address) ([]common.Address, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	out0, err := chainTraffic.traffic.GetTransferredAddress(opts, address)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedAddress(address common.Address) ([]common.Address, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	out0, err := chainTraffic.traffic.GetRetrievedAddress(opts, address)
	return out0, err
}

func (chainTraffic *ChainTraffic) BalanceOf(account common.Address) (*big.Int, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	out0, err := chainTraffic.traffic.BalanceOf(opts, account)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedTotal(arg0 common.Address) (*big.Int, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	out0, err := chainTraffic.traffic.RetrievedTotal(opts, arg0)
	return out0, err
}

func (chainTraffic *ChainTraffic) TransferredTotal(arg0 common.Address) (*big.Int, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	out0, err := chainTraffic.traffic.TransferredTotal(opts, arg0)
	return out0, err
}

func (chainTraffic *ChainTraffic) TransAmount(beneficiary, recipient common.Address) (*big.Int, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	opts := &bind.CallOpts{Context: ctx}
	return chainTraffic.traffic.TransTraffic(opts, beneficiary, recipient)
}

func (chainTraffic *ChainTraffic) CashChequeBeneficiary(ctx context.Context, beneficiary, recipient common.Address, cumulativePayout *big.Int, signature []byte) (*types.Transaction, error) {
	nonce, err := chainTraffic.transactionService.NextNonce(ctx)
	if err != nil {
		return nil, err
	}

	gasPrice, err := chainTraffic.backend.SuggestGasPrice(ctx)
	if err != nil {
		return nil, err
	}

	opts := &bind.TransactOpts{
		From: beneficiary,
		Signer: func(address common.Address, tx *types.Transaction) (*types.Transaction, error) {
			if address != beneficiary {
				return nil, bind.ErrNotAuthorized
			}
			return chainTraffic.signer.SignTx(tx, chainTraffic.chainID)
		},
		GasLimit: 1000000,
		GasPrice: gasPrice,
		Context:  ctx,
		Nonce:    new(big.Int).SetUint64(nonce),
	}
	return chainTraffic.traffic.CashChequeBeneficiary(opts, beneficiary, recipient, cumulativePayout, signature)
}
