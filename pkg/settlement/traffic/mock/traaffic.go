package mock

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
)

type TraafficMock struct {
	// LastSentCheque returns the last sent cheque for the peer
	lastSentCheque func(peer boson.Address) (*chequePkg.Cheque, error)
	// LastReceivedCheques returns the list of last received cheques for all peers
	lastReceivedCheque func(peer boson.Address) (*chequePkg.SignedCheque, error)
	// CashCheque sends a cashing transaction for the last cheque of the peer
	cashCheque func(ctx context.Context, peer boson.Address) (common.Hash, error)

	trafficCheques func() ([]*traffic.TrafficCheque, error)

	address func() common.Address

	trafficInfo func() (*traffic.TrafficInfo, error)
}

// WithsettlementFunc sets the mock settlement function
func WithLastSentCheque(f func(peer boson.Address) (*chequePkg.Cheque, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.lastSentCheque = f
	})
}

func WithLastReceivedCheque(f func(peer boson.Address) (*chequePkg.SignedCheque, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.lastReceivedCheque = f
	})
}

func WithCashCheque(f func(ctx context.Context, peer boson.Address) (common.Hash, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.cashCheque = f
	})
}

func WithTrafficCheques(f func() ([]*traffic.TrafficCheque, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.trafficCheques = f
	})
}

func WithAddress(f func() common.Address) Option {
	return optionFunc(func(s *TraafficMock) {
		s.address = f
	})
}

func WithTrafficInfo(f func() common.Address) Option {
	return optionFunc(func(s *TraafficMock) {
		s.address = f
	})
}

// LastSentCheque returns the last sent cheque for the peer
func (s *TraafficMock) LastSentCheque(peer boson.Address) (*chequePkg.Cheque, error) {
	return s.lastSentCheque(peer)
}

// LastReceivedCheques returns the list of last received cheques for all peers
func (s *TraafficMock) LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error) {
	return s.lastReceivedCheque(peer)
}

// CashCheque sends a cashing transaction for the last cheque of the peer
func (s *TraafficMock) CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error) {
	return s.cashCheque(ctx, peer)
}

func (s *TraafficMock) TrafficCheques() ([]*traffic.TrafficCheque, error) {
	return s.trafficCheques()
}

func (s *TraafficMock) Address() common.Address {
	return s.address()
}

func (s *TraafficMock) TrafficInfo() (*traffic.TrafficInfo, error) {
	return s.trafficInfo()
}

// Option is the option passed to the mock ChequeStore service
type Option interface {
	apply(*TraafficMock)
}

type optionFunc func(*TraafficMock)

func (f optionFunc) apply(r *TraafficMock) { f(r) }
