package mock

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
)

type ChainTrafficMock struct {
	transferredAddress func(address common.Address) ([]common.Address, error)

	retrievedAddress func(address common.Address) ([]common.Address, error)

	balanceOf func(account common.Address) (*big.Int, error)

	retrievedTotal func(address common.Address) (*big.Int, error)

	transferredTotal func(address common.Address) (*big.Int, error)

	transAmount func(beneficiary, recipient common.Address) (*big.Int, error)

	cashChequeBeneficiary func(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, signature []byte) (*types.Transaction, error)
}

func (m *ChainTrafficMock) TransferredAddress(address common.Address) ([]common.Address, error) {
	if m.transferredAddress != nil {
		return m.transferredAddress(address)
	}
	return []common.Address{}, errors.New("not implemented")
}

func (m *ChainTrafficMock) RetrievedAddress(address common.Address) ([]common.Address, error) {
	if m.retrievedAddress != nil {
		return m.retrievedAddress(address)
	}
	return []common.Address{}, errors.New("not implemented")
}

func (m *ChainTrafficMock) BalanceOf(address common.Address) (*big.Int, error) {
	if m.balanceOf != nil {
		return m.balanceOf(address)
	}
	return big.NewInt(0), errors.New("not implemented")
}

func (m *ChainTrafficMock) RetrievedTotal(address common.Address) (*big.Int, error) {
	if m.retrievedTotal != nil {
		return m.retrievedTotal(address)
	}
	return big.NewInt(0), errors.New("not implemented")
}

func (m *ChainTrafficMock) TransferredTotal(address common.Address) (*big.Int, error) {
	if m.transferredTotal != nil {
		return m.transferredTotal(address)
	}
	return big.NewInt(0), errors.New("not implemented")
}

func (m *ChainTrafficMock) TransAmount(beneficiary, recipient common.Address) (*big.Int, error) {
	if m.transAmount != nil {
		return m.transAmount(beneficiary, recipient)
	}
	return big.NewInt(0), errors.New("not implemented")
}

func (m *ChainTrafficMock) CashChequeBeneficiary(ctx context.Context, peer boson.Address, beneficiary, recipient common.Address, cumulativePayout *big.Int, signature []byte) (*types.Transaction, error) {
	if m.cashChequeBeneficiary != nil {
		return m.cashChequeBeneficiary(ctx, peer, beneficiary, recipient, cumulativePayout, signature)
	}
	return nil, errors.New("not implemented")
}

func New(opts ...Option) chain.Traffic {
	mock := new(ChainTrafficMock)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
}

// Option is the option passed to the mock Chequebook service
type Option interface {
	apply(*ChainTrafficMock)
}

func WithTransferredAddress(f func(address common.Address) ([]common.Address, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.transferredAddress = f
	})
}

func WithRetrievedAddress(f func(address common.Address) ([]common.Address, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.retrievedAddress = f
	})
}

func WithBalanceOf(f func(account common.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.balanceOf = f
	})
}

func WithRetrievedTotal(f func(address common.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.retrievedTotal = f
	})
}

func WithTransferredTotal(f func(address common.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.transferredTotal = f
	})
}

func WithTransAmount(f func(beneficiary, recipient common.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.transAmount = f
	})
}

func WithCashChequeBeneficiary(f func(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, signature []byte) (*types.Transaction, error)) Option {
	return optionFunc(func(s *ChainTrafficMock) {
		s.cashChequeBeneficiary = f
	})
}

type optionFunc func(*ChainTrafficMock)

func (f optionFunc) apply(r *ChainTrafficMock) { f(r) }
