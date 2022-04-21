package traffic

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
	"sync"
	"time"
)

type ChainTraffic struct {
	sync.Mutex
	logger             logging.Logger
	signer             crypto.Signer
	chainID            *big.Int
	backend            *ethclient.Client
	traffic            *Traffic
	transactionService chain.Transaction
	commonService      chain.Common
}

func NewServer(logger logging.Logger, chainID *big.Int, backend *ethclient.Client, signer crypto.Signer,
	transactionService chain.Transaction, address string, commonService chain.Common) (chain.Traffic, error) {

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
		commonService:      commonService,
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

func (chainTraffic *ChainTraffic) CashChequeBeneficiary(ctx context.Context, peer boson.Address, beneficiary, recipient common.Address, cumulativePayout *big.Int, signature []byte) (tx *types.Transaction, err error) {
	chainTraffic.Lock()
	defer chainTraffic.Unlock()
	defer func() {
		if err == nil {
			chainTraffic.commonService.SyncTransaction(chain.TRAFFIC, peer.String(), tx.Hash().String())
		}
	}()
	if chainTraffic.commonService.IsTransaction() {
		return nil, errors.New("existing chain transaction")
	}
	nonce, err := chainTraffic.transactionService.NextNonce(ctx)
	if err != nil {
		return nil, err
	}
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	gasPrice, err := chainTraffic.backend.SuggestGasPrice(ctx1)
	if err != nil {
		return nil, err
	}

	ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
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
		Context:  ctx2,
		Nonce:    new(big.Int).SetUint64(nonce),
	}
	return chainTraffic.traffic.CashChequeBeneficiary(opts, beneficiary, recipient, cumulativePayout, signature)
}
