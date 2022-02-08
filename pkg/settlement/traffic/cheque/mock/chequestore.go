package mock

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"math/big"
)

// Service is the mock chequeStore service.
type ChequeStoreMock struct {
	// ReceiveCheque verifies and stores a cheque. It returns the totam amount earned.
	receiveCheque func(ctx context.Context, cheque *cheque.SignedCheque) (*big.Int, error)

	putSendCheque func(ctx context.Context, cheque *cheque.Cheque, chainAddress common.Address) error
	// LastReceivedCheque returns the last cheque we received from a specific chainAddress.
	lastReceivedCheque func(chainAddress common.Address) (*cheque.SignedCheque, error)
	// LastReceivedCheques returns the last received cheques from every known chainAddress.
	lastReceivedCheques func() (map[common.Address]*cheque.SignedCheque, error)

	lastSendCheque func(chainAddress common.Address) (*cheque.Cheque, error)

	lastSendCheques func() (map[common.Address]*cheque.Cheque, error)

	putRetrieveTraffic func(chainAddress common.Address, traffic *big.Int) error

	putTransferTraffic func(chainAddress common.Address, traffic *big.Int) error

	putReceivedCheques func(chainAddress common.Address, cheque cheque.SignedCheque) error
}

func (s *ChequeStoreMock) GetRetrieveTraffic(chainAddress common.Address) (traffic *big.Int, err error) {
	panic("implement me")
}

func (s *ChequeStoreMock) GetTransferTraffic(chainAddress common.Address) (traffic *big.Int, err error) {
	panic("implement me")
}

func WithRetrieveChequeFunc(f func(ctx context.Context, cheque *cheque.SignedCheque) (*big.Int, error)) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.receiveCheque = f
	})
}

func WithPutSendCheque(f func(ctx context.Context, cheque *cheque.Cheque, chainAddress common.Address) error) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.putSendCheque = f
	})
}

func WithLastReceivedCheque(f func(chainAddress common.Address) (*cheque.SignedCheque, error)) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.lastReceivedCheque = f
	})
}

func WithLastReceivedCheques(f func() (map[common.Address]*cheque.SignedCheque, error)) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.lastReceivedCheques = f
	})
}

func WithLastSendCheque(f func(chainAddress common.Address) (*cheque.Cheque, error)) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.lastSendCheque = f
	})
}

func WithLastSendCheques(f func() (map[common.Address]*cheque.Cheque, error)) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.lastSendCheques = f
	})
}

func WithPutRetrieveTraffic(f func(chainAddress common.Address, traffic *big.Int) error) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.putRetrieveTraffic = f
	})
}

func WithPutTransferTraffic(f func(chainAddress common.Address, traffic *big.Int) error) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.putTransferTraffic = f
	})
}

func WithPutReceivedCheques(f func(chainAddress common.Address, cheque cheque.SignedCheque) error) Option {
	return optionFunc(func(s *ChequeStoreMock) {
		s.putReceivedCheques = f
	})
}

// NewChequeStore creates the mock chequeStore implementation
func NewChequeStore(opts ...Option) cheque.ChequeStore {
	mock := new(ChequeStoreMock)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
}

// ReceiveCheque verifies and stores a cheque. It returns the totam amount earned.
func (s *ChequeStoreMock) ReceiveCheque(ctx context.Context, cheque *cheque.SignedCheque) (*big.Int, error) {
	return s.receiveCheque(ctx, cheque)
}

func (s *ChequeStoreMock) PutSendCheque(ctx context.Context, cheque *cheque.Cheque, chainAddress common.Address) error {
	return s.putSendCheque(ctx, cheque, chainAddress)
}

// LastReceivedCheque returns the last cheque we received from a specific chainAddress.
func (s *ChequeStoreMock) LastReceivedCheque(chainAddress common.Address) (*cheque.SignedCheque, error) {
	return s.lastReceivedCheque(chainAddress)
}

// LastReceivedCheques returns the last received cheques from every known chainAddress.
func (s *ChequeStoreMock) LastReceivedCheques() (map[common.Address]*cheque.SignedCheque, error) {
	return s.lastReceivedCheques()
}

func (s *ChequeStoreMock) LastSendCheque(chainAddress common.Address) (*cheque.Cheque, error) {
	return s.lastSendCheque(chainAddress)
}

func (s *ChequeStoreMock) LastSendCheques() (map[common.Address]*cheque.Cheque, error) {
	return s.lastSendCheques()
}

func (s *ChequeStoreMock) PutRetrieveTraffic(chainAddress common.Address, traffic *big.Int) error {
	return s.putRetrieveTraffic(chainAddress, traffic)
}

func (s *ChequeStoreMock) PutTransferTraffic(chainAddress common.Address, traffic *big.Int) error {
	return s.putTransferTraffic(chainAddress, traffic)
}

func (s *ChequeStoreMock) PutReceivedCheques(chainAddress common.Address, cheque cheque.SignedCheque) error {
	return s.putReceivedCheques(chainAddress, cheque)
}

func (s *ChequeStoreMock) VerifyCheque(cheque *cheque.SignedCheque, chainID int64) (common.Address, error) {
	return common.HexToAddress(""), nil
}

// Option is the option passed to the mock ChequeStore service
type Option interface {
	apply(*ChequeStoreMock)
}

type optionFunc func(*ChequeStoreMock)

func (f optionFunc) apply(r *ChequeStoreMock) { f(r) }
