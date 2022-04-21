// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction

import (
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"golang.org/x/net/context"
)

const (
	noncePrefix = "transaction_nonce_"
)

var (
	// ErrTransactionReverted denotes that the sent transaction has been
	// reverted.
	ErrTransactionReverted = errors.New("transaction reverted")
)

type transactionService struct {
	lock sync.Mutex

	logger        logging.Logger
	backend       Backend
	signer        crypto.Signer
	sender        common.Address
	store         storage.StateStorer
	chainID       *big.Int
	commonService chain.Common
}

// NewService creates a new transaction service.
func NewService(logger logging.Logger, backend Backend, signer crypto.Signer, store storage.StateStorer, commonService chain.Common, chainID *big.Int) (chain.Transaction, error) {
	senderAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}

	return &transactionService{
		logger:        logger,
		backend:       backend,
		signer:        signer,
		sender:        senderAddress,
		store:         store,
		chainID:       chainID,
		commonService: commonService,
	}, nil
}

// Send creates and signs a transaction based on the request and sends it.
func (t *transactionService) Send(ctx context.Context, request *chain.TxRequest) (txHash common.Hash, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	nonce, err := t.NextNonce(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	tx, err := t.prepareTransaction(ctx, request, nonce)
	if err != nil {
		return common.Hash{}, err
	}
	signedTx, err := t.signer.SignTx(tx, t.chainID)
	if err != nil {
		return common.Hash{}, err
	}

	t.logger.Tracef("sending transaction %x with nonce %d", signedTx.Hash(), nonce)

	err = t.backend.SendTransaction(ctx, signedTx)
	if err != nil {
		return common.Hash{}, err
	}

	err = t.putNonce(nonce + 1)
	if err != nil {
		return common.Hash{}, err
	}

	return signedTx.Hash(), nil
}

func (t *transactionService) Call(ctx context.Context, request *chain.TxRequest) ([]byte, error) {
	msg := ethereum.CallMsg{
		From:     t.sender,
		To:       request.To,
		Data:     request.Data,
		GasPrice: request.GasPrice,
		Gas:      request.GasLimit,
		Value:    request.Value,
	}

	data, err := t.backend.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// WaitForReceipt waits until either the transaction with the given hash has
// been mined or the context is cancelled.
func (t *transactionService) WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	defer func() {
		t.commonService.UpdateStatus(false)
	}()
	for {
		receipt, err := t.backend.TransactionReceipt(ctx, txHash)
		if receipt != nil {
			return receipt, nil
		}
		if err != nil {
			// some node implementations return an error if the transaction is not yet mined
			t.logger.Tracef("waiting for transaction %x to be mined: %v", txHash, err)
		} else {
			t.logger.Tracef("waiting for transaction %x to be mined", txHash)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// prepareTransaction creates a signable transaction based on a request.
func (t *transactionService) prepareTransaction(ctx context.Context, request *chain.TxRequest, nonce uint64) (tx *types.Transaction, err error) {
	var gasLimit uint64
	if request.GasLimit == 0 {
		gasLimit, err = t.backend.EstimateGas(ctx, ethereum.CallMsg{
			From: t.sender,
			To:   request.To,
			Data: request.Data,
		})
		if err != nil {
			return nil, err
		}
	} else {
		gasLimit = request.GasLimit
	}

	var gasPrice *big.Int
	if request.GasPrice == nil {
		gasPrice, err = t.backend.SuggestGasPrice(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		gasPrice = request.GasPrice
	}

	return types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       request.To,
		Value:    request.Value,
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     request.Data,
	}), nil
}

func (t *transactionService) nonceKey() string {
	return fmt.Sprintf("%s%x", noncePrefix, t.sender)
}

func (t *transactionService) NextNonce(ctx context.Context) (uint64, error) {
	ctx, cance := context.WithTimeout(ctx, 2*time.Second)
	defer cance()
	onchainNonce, err := t.backend.PendingNonceAt(ctx, t.sender)
	if err != nil {
		return 0, err
	}
	return onchainNonce, nil
}

func (t *transactionService) putNonce(nonce uint64) error {
	return t.store.Put(t.nonceKey(), nonce)
}
