// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type transactionServiceMock struct {
	send           func(ctx context.Context, request *chain.TxRequest) (txHash common.Hash, err error)
	waitForReceipt func(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)
	call           func(ctx context.Context, request *chain.TxRequest) (result []byte, err error)
}

func (m *transactionServiceMock) Send(ctx context.Context, request *chain.TxRequest) (txHash common.Hash, err error) {
	if m.send != nil {
		return m.send(ctx, request)
	}
	return common.Hash{}, errors.New("not implemented")
}

func (m *transactionServiceMock) WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	if m.waitForReceipt != nil {
		return m.waitForReceipt(ctx, txHash)
	}
	return nil, errors.New("not implemented")
}

func (m *transactionServiceMock) Call(ctx context.Context, request *chain.TxRequest) (result []byte, err error) {
	if m.call != nil {
		return m.call(ctx, request)
	}
	return nil, errors.New("not implemented")
}

func (m *transactionServiceMock) NextNonce(ctx context.Context) (uint64, error) {
	return 1, nil
}

// Option is the option passed to the mock Chequebook service
type Option interface {
	apply(*transactionServiceMock)
}

type optionFunc func(*transactionServiceMock)

func (f optionFunc) apply(r *transactionServiceMock) { f(r) }

func WithSendFunc(f func(ctx context.Context, request *chain.TxRequest) (txHash common.Hash, err error)) Option {
	return optionFunc(func(s *transactionServiceMock) {
		s.send = f
	})
}

func WithWaitForReceiptFunc(f func(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)) Option {
	return optionFunc(func(s *transactionServiceMock) {
		s.waitForReceipt = f
	})
}

func WithCallFunc(f func(ctx context.Context, request *chain.TxRequest) (result []byte, err error)) Option {
	return optionFunc(func(s *transactionServiceMock) {
		s.call = f
	})
}

func New(opts ...Option) chain.Transaction {
	mock := new(transactionServiceMock)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
}

type Call struct {
	abi    *abi.ABI
	result []byte
	method string
	params []interface{}
}

func ABICall(abi *abi.ABI, result []byte, method string, params ...interface{}) Call {
	return Call{
		abi:    abi,
		result: result,
		method: method,
		params: params,
	}
}

func WithABICallSequence(calls ...Call) Option {
	return optionFunc(func(s *transactionServiceMock) {
		s.call = func(ctx context.Context, request *chain.TxRequest) ([]byte, error) {
			if len(calls) == 0 {
				return nil, errors.New("unexpected call")
			}

			call := calls[0]

			data, err := call.abi.Pack(call.method, call.params...)
			if err != nil {
				return nil, err
			}

			if !bytes.Equal(data, request.Data) {
				return nil, fmt.Errorf("wrong data. wanted %x, got %x", data, request.Data)
			}

			calls = calls[1:]

			return call.result, nil
		}
	})
}

func WithABICall(abi *abi.ABI, result []byte, method string, params ...interface{}) Option {
	return WithABICallSequence(ABICall(abi, result, method, params...))
}

func WithABISend(abi *abi.ABI, txHash common.Hash, expectedAddress common.Address, expectedValue *big.Int, method string, params ...interface{}) Option {
	return optionFunc(func(s *transactionServiceMock) {
		s.send = func(ctx context.Context, request *chain.TxRequest) (common.Hash, error) {
			data, err := abi.Pack(method, params...)
			if err != nil {
				return common.Hash{}, err
			}

			_ = data

			return txHash, nil
		}
	})
}
