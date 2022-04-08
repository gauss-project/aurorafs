package mock

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/settlement"
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

	pay func(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error

	transferTraffic func(peer boson.Address) (traffic *big.Int, err error)

	retrieveTraffic func(peer boson.Address) (traffic *big.Int, err error)

	putRetrieveTraffic func(peer boson.Address, traffic *big.Int) error

	putTransferTraffic func(peer boson.Address, traffic *big.Int) error

	// AvailableBalance Get actual available balance
	availableBalance func() (*big.Int, error)

	// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
	setNotifyPaymentFunc func(notifyPaymentFunc settlement.NotifyPaymentFunc)

	getPeerBalance func(peer boson.Address) (*big.Int, error)

	getUnPaidBalance func(peer boson.Address) (*big.Int, error)
}

func (s *TraafficMock) API() rpc.API {
	return rpc.API{}
}

func (s *TraafficMock) TrafficInit() error {
	panic("implement me")
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

func WithPay(f func(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error) Option {
	return optionFunc(func(s *TraafficMock) {
		s.pay = f
	})
}

func WithTransferTraffic(f func(peer boson.Address) (traffic *big.Int, err error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.transferTraffic = f
	})
}

func WithRetrieveTraffic(f func(peer boson.Address) (traffic *big.Int, err error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.retrieveTraffic = f
	})
}

func WithPutRetrieveTraffic(f func(peer boson.Address, traffic *big.Int) error) Option {
	return optionFunc(func(s *TraafficMock) {
		s.putRetrieveTraffic = f
	})
}

func WithPutTransferTraffic(f func(peer boson.Address, traffic *big.Int) error) Option {
	return optionFunc(func(s *TraafficMock) {
		s.putTransferTraffic = f
	})
}

func WithAvailableBalance(f func() (*big.Int, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.availableBalance = f
	})
}

func WithSetNotifyPaymentFunc(f func(notifyPaymentFunc settlement.NotifyPaymentFunc)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.setNotifyPaymentFunc = f
	})
}

func WithGetPeerBalance(f func(peer boson.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.getPeerBalance = f
	})
}

func WithGetUnPaidBalance(f func(peer boson.Address) (*big.Int, error)) Option {
	return optionFunc(func(s *TraafficMock) {
		s.getUnPaidBalance = f
	})
}

// NewChequeStore creates the mock chequeStore implementation
func NewSettlement(opts ...Option) settlement.Interface {
	mock := new(TraafficMock)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
}

// NewTraffic creates the mock chequeStore implementation
func NewTraffic(opts ...Option) traffic.ApiInterface {
	mock := new(TraafficMock)
	for _, o := range opts {
		o.apply(mock)
	}
	return mock
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

func (s *TraafficMock) Pay(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error {
	return s.pay(ctx, peer, paymentThreshold)
}

func (s *TraafficMock) TransferTraffic(peer boson.Address) (traffic *big.Int, err error) {
	return s.transferTraffic(peer)
}

func (s *TraafficMock) RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error) {
	return s.retrieveTraffic(peer)
}

func (s *TraafficMock) PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error {
	return s.putRetrieveTraffic(peer, traffic)
}

func (s *TraafficMock) PutTransferTraffic(peer boson.Address, traffic *big.Int) error {
	return s.putTransferTraffic(peer, traffic)
}

// AvailableBalance Get actual available balance
func (s *TraafficMock) AvailableBalance() (*big.Int, error) {
	return s.availableBalance()
}

// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
func (s *TraafficMock) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.setNotifyPaymentFunc(notifyPaymentFunc)
}

func (s *TraafficMock) GetPeerBalance(peer boson.Address) (*big.Int, error) {
	return s.getPeerBalance(peer)
}

func (s *TraafficMock) GetUnPaidBalance(peer boson.Address) (*big.Int, error) {
	return s.getUnPaidBalance(peer)
}

// Option is the option passed to the mock ChequeStore service
type Option interface {
	apply(*TraafficMock)
}

type optionFunc func(*TraafficMock)

func (f optionFunc) apply(r *TraafficMock) { f(r) }
