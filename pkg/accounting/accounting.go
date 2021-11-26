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
	_                     Interface = (*Accounting)(nil)
	balancesPrefix        string    = "accounting_balance_"
	balancesSurplusPrefix string    = "accounting_surplusbalance_"

	// ErrDisconnectThresholdExceeded denotes a peer has exceeded the disconnect threshold.
	ErrDisconnectThresholdExceeded = errors.New("disconnect threshold exceeded")
)

// Interface is the Accounting interface.
type Interface interface {
	Reserve(ctx context.Context, peer boson.Address, traffic uint64) error
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
	peer                      boson.Address
	balance, paymentThreshold *big.Int
}

type accountingPeer struct {
	lock             sync.Mutex
	paymentThreshold *big.Int
	unPayTraffic     *big.Int
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
		payChan:          make(chan payChan, 4),
		metrics:          newMetrics(),
	}
	go acc.settle()
	return acc
}

// Reserve reserves a portion of the balance for peer and attempts settlements if necessary.
func (a *Accounting) Reserve(ctx context.Context, peer boson.Address, traffic uint64) error {

Loop:
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	retrieve := accountingPeer.unPayTraffic

	if retrieve.Cmp(big.NewInt(0)) == 0 {
		retrieve, err = a.settlement.RetrieveTraffic(peer)
		if err != nil {
			return err
		}
	}

	retrieve = retrieve.Add(retrieve, new(big.Int).SetUint64(traffic))
	available, err := a.settlement.AvailableBalance(ctx)
	if err != nil {
		return err
	}

	if available.Cmp(retrieve) > 0 {
		return fmt.Errorf("low available balance")
	}

	if retrieve.Cmp(a.paymentTolerance) >= 0 {
		// todo prevent frequent loop logic
		goto Loop
	}
	accountingPeer.unPayTraffic = retrieve

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
	if err := a.settlement.PutRetrieveTraffic(peer, new(big.Int).SetUint64(traffic)); err != nil {
		a.logger.Errorf("failed to modify retrieve traffic")
		return err
	}
	balance, err := a.settlement.RetrieveTraffic(peer)
	balance = balance.Add(balance, new(big.Int).SetUint64(traffic))
	if balance.Cmp(accountingPeer.paymentThreshold) >= 0 {
		a.payChan <- payChan{
			peer:             peer,
			balance:          balance,
			paymentThreshold: accountingPeer.paymentThreshold,
		}
	}

	return nil
}

// Settle all debt with a peer. The lock on the accountingPeer must be held when
// called.
func (a *Accounting) settle() {

	for {
		pay := <-a.payChan
		if err := a.settlement.Pay(context.Background(), pay.peer, pay.balance, pay.paymentThreshold); err != nil {
			a.logger.Errorf("generating check errors %v", err)
		}
	}
}

// Debit increases the amount of debt we have with the given peer (and decreases
// existing credit).
func (a *Accounting) Debit(peer boson.Address, traffic uint64) error {

	tolerance := a.paymentTolerance
	traff, err := a.settlement.TransferTraffic(peer)
	if err != nil {
		return err
	}
	if tolerance.Cmp(traff) >= 0 {
		a.metrics.AccountingDisconnectsCount.Inc()
		return p2p.NewBlockPeerError(10000*time.Hour, ErrDisconnectThresholdExceeded)
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
		a.accountingPeers[peer.String()] = &accountingPeer{
			paymentThreshold: a.paymentThreshold,
			unPayTraffic:     big.NewInt(0),
		}
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
	unPay := accountingPeer.unPayTraffic
	if unPay.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}
	if unPay.Cmp(traffic) < 0 {
		return fmt.Errorf("unpaid traffic is less than paid traffic")
	}
	accountingPeer.unPayTraffic = unPay.Sub(unPay, traffic)
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
