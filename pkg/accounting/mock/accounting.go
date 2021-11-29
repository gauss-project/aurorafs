// Package mock provides a mock implementation for the
// accounting interface.
package mock

import (
	"context"
	"math/big"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/accounting"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

// Service is the mock Accounting service.
type Service struct {
	lock        sync.Mutex
	balances    map[string]*big.Int
	reserveFunc func(ctx context.Context, peer boson.Address, price uint64) error
	creditFunc  func(peer boson.Address, price uint64) error
	debitFunc   func(peer boson.Address, price uint64) error
}

// NewAccounting creates the mock accounting implementation
func NewAccounting(opts ...Option) accounting.Interface {
	mock := new(Service)
	mock.balances = make(map[string]*big.Int)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
}

// Reserve is the mock function wrapper that calls the set implementation
func (s *Service) Reserve(ctx context.Context, peer boson.Address, traffic uint64) error {
	if s.reserveFunc != nil {
		return s.reserveFunc(ctx, peer, traffic)
	}
	return nil
}

// Credit is the mock function wrapper that calls the set implementation
func (s *Service) Credit(peer boson.Address, traffic uint64) error {
	if s.creditFunc != nil {
		return s.creditFunc(peer, traffic)
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if bal, ok := s.balances[peer.String()]; ok {
		s.balances[peer.String()] = new(big.Int).Sub(bal, new(big.Int).SetUint64(traffic))
	} else {
		s.balances[peer.String()] = big.NewInt(-int64(traffic))
	}
	return nil
}

// Debit is the mock function wrapper that calls the set implementation
func (s *Service) Debit(peer boson.Address, traffic uint64) error {
	if s.debitFunc != nil {
		return s.debitFunc(peer, traffic)
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if bal, ok := s.balances[peer.String()]; ok {
		s.balances[peer.String()] = new(big.Int).Add(bal, new(big.Int).SetUint64(traffic))
	} else {
		s.balances[peer.String()] = new(big.Int).SetUint64(traffic)
	}
	return nil
}

// Option is the option passed to the mock accounting service
type Option interface {
	apply(*Service)
}

type optionFunc func(*Service)

func (f optionFunc) apply(r *Service) { f(r) }
