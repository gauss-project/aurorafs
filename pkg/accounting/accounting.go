// Package accounting provides functionalities needed
// to do per-peer accounting.
package accounting

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"math/big"
	"sync"
	"time"
)

var (
	_ Interface = (*Accounting)(nil)

	// ErrDisconnectThresholdExceeded denotes a peer has exceeded the disconnect threshold.
	ErrDisconnectThresholdExceeded = errors.New("disconnect threshold exceeded")
	ErrLowAvailableExceeded        = errors.New("low available balance")
)

// Interface is the Accounting interface.
type Interface interface {
	Reserve(peer boson.Address, traffic uint64) error
	// Credit increases the balance the peer has with us (we "pay" the peer).
	Credit(ctx context.Context, peer boson.Address, traffic uint64) error
	// Debit increases the balance we have with the peer (we get "paid" back).
	Debit(peer boson.Address, traffic uint64) error
}

// Accounting is the main implementation of the accounting interface.
type Accounting struct {
	// Mutex for accessing the accountingPeers map.
	accountingPeersMu sync.Mutex
	accountingPeers   map[string]*accountingPeer
	logger            logging.Logger
	store             storage.StateStorer
	paymentTolerance  *big.Int
	paymentThreshold  *big.Int
	settlement        settlement.Interface
	metrics           metrics
	payChan           chan payChan
}

type payChan struct {
	peer             boson.Address
	paymentThreshold *big.Int
}

type accountingPeer struct {
	lock             sync.Mutex
	paymentThreshold *big.Int
	unPaidTraffic    *big.Int
}

// NewAccounting creates a new Accounting instance with the provided options.
func NewAccounting(
	paymentTolerance,
	paymentThreshold *big.Int,
	logger logging.Logger,
	store storage.StateStorer,
	settlement settlement.Interface,
) *Accounting {
	acc := &Accounting{
		accountingPeers:  make(map[string]*accountingPeer),
		paymentTolerance: new(big.Int).Set(paymentTolerance),
		paymentThreshold: new(big.Int).Set(paymentThreshold),
		logger:           logger,
		store:            store,
		settlement:       settlement,
		payChan:          make(chan payChan, 1000),
		metrics:          newMetrics(),
	}
	go acc.settle()
	return acc
}

// Reserve reserves a portion of the balance for peer and attempts settlements if necessary.
func (a *Accounting) Reserve(peer boson.Address, traffic uint64) (err error) {
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	retrieve := accountingPeer.unPaidTraffic
	ret := big.NewInt(0).Add(retrieve, new(big.Int).SetUint64(traffic))
	available, err := a.settlement.AvailableBalance()
	if err != nil {
		return err
	}
	if available.Cmp(ret) < 0 {
		return ErrLowAvailableExceeded
	}

	return nil
}

// Credit increases the amount of credit we have with the given peer
// (and decreases existing debt).
func (a *Accounting) Credit(ctx context.Context, peer boson.Address, traffic uint64) error {
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	accountingPeer.unPaidTraffic = big.NewInt(0).Add(accountingPeer.unPaidTraffic, new(big.Int).SetUint64(traffic))
	if err := a.settlement.PutRetrieveTraffic(peer, new(big.Int).SetUint64(traffic)); err != nil {
		a.logger.Errorf("failed to modify retrieve traffic")
		return err
	}
	unPaid := accountingPeer.unPaidTraffic
	if unPaid.Cmp(accountingPeer.paymentThreshold) >= 0 {
		a.payChan <- payChan{
			peer:             peer,
			paymentThreshold: accountingPeer.paymentThreshold,
		}
	}

	return nil
}

// Settle all debt with a peer. The lock on the accountingPeer must be held when
// called.
func (a *Accounting) settle() {

	for pay := range a.payChan {
		if err := a.settlement.Pay(context.Background(), pay.peer, pay.paymentThreshold); err != nil {
			a.logger.Errorf("generating check errors %v", err)
		}
	}
}

// Debit increases the amount of debt we have with the given peer (and decreases
// existing credit).
func (a *Accounting) Debit(peer boson.Address, traffic uint64) error {

	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	tolerance := a.paymentTolerance
	traff, err := a.settlement.TransferTraffic(peer)
	if err != nil {
		return err
	}
	if tolerance.Cmp(traff) <= 0 {
		a.metrics.AccountingDisconnectsCount.Inc()
		a.logger.Errorf("block list %s", peer.String())
		//return ErrDisconnectThresholdExceeded
		return p2p.NewBlockPeerError(24*time.Hour, ErrDisconnectThresholdExceeded)
	}

	balance, err := a.settlement.GetPeerBalance(peer)
	if err != nil {
		return err
	}
	unPaid, err := a.settlement.GetUnPaidBalance(peer)
	if err != nil {
		return err
	}
	if balance.Cmp(unPaid) < 0 {
		return fmt.Errorf("low node traffic balance: %s, traffic: %d, unpaid: %d ", peer.String(), balance, unPaid)
	}
	if err := a.settlement.PutTransferTraffic(peer, new(big.Int).SetUint64(traffic)); err != nil {
		return err
	}
	return nil
}

// getAccountingPeer returns the accountingPeer for a given boson address.
// If not found in memory it will initialize it.
func (a *Accounting) getAccountingPeer(peer boson.Address) (*accountingPeer, error) {
	a.accountingPeersMu.Lock()
	defer a.accountingPeersMu.Unlock()
	peerData, ok := a.accountingPeers[peer.String()]
	if !ok {
		retrieve, err := a.settlement.RetrieveTraffic(peer)
		if err != nil {
			return nil, err
		}
		peerData = &accountingPeer{
			paymentThreshold: a.paymentThreshold,
			unPaidTraffic:    retrieve,
		}
		a.accountingPeers[peer.String()] = peerData
	}
	return peerData, nil
}

func (a *Accounting) NotifyPayment(peer boson.Address, traffic *big.Int) error {
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}

	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	unPay := accountingPeer.unPaidTraffic
	if unPay.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}
	if unPay.Cmp(traffic) < 0 {
		accountingPeer.unPaidTraffic = big.NewInt(0)
	} else {
		accountingPeer.unPaidTraffic = new(big.Int).Sub(unPay, traffic)
	}
	return nil

}
func (a *Accounting) AsyncNotifyPayment(peer boson.Address, traffic *big.Int) error {

	go func() {
		err := a.NotifyPayment(peer, traffic)
		if err != nil {
			a.logger.Errorf("failed to notify accounting of payment: %v", err)
		}
	}()
	return nil
}
