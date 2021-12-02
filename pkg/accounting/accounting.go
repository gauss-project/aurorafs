// Package accounting provides functionalities needed
// to do per-peer accounting.
package accounting

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"math/big"
	"sync"
)

var (
	_                     Interface = (*Accounting)(nil)
	balancesPrefix        string    = "accounting_balance_"
	balancesSurplusPrefix string    = "accounting_surplusbalance_"

	// ErrDisconnectThresholdExceeded denotes a peer has exceeded the disconnect threshold.
	ErrDisconnectThresholdExceeded = errors.New("disconnect threshold exceeded")
	ErrLowAvailableExceeded        = errors.New("low available balance")
)

// Interface is the Accounting interface.
type Interface interface {
	Reserve(ctx context.Context, peer boson.Address, traffic uint64) error
	// Credit increases the balance the peer has with us (we "pay" the peer).
	Credit(peer boson.Address, traffic uint64) error
	// Debit increases the balance we have with the peer (we get "paid" back).
	Debit(peer boson.Address, traffic uint64) error
}

// Accounting is the main implementation of the accounting interface.
type Accounting struct {
	// Mutex for accessing the accountingPeers map.
	accountingMu      sync.Mutex
	accountingPeersMu sync.Mutex
	accountingPeers   map[string]*accountingPeer
	logger            logging.Logger
	store             storage.StateStorer
	paymentTolerance  *big.Int
	paymentThreshold  *big.Int
	settlement        settlement.Interface
	metrics           metrics
	payChan           chan payChan
	reserveChan       chan bool
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
		payChan:          make(chan payChan, 100),
		metrics:          newMetrics(),
	}
	go acc.settle()
	return acc
}

// Reserve reserves a portion of the balance for peer and attempts settlements if necessary.
func (a *Accounting) Reserve(ctx context.Context, peer boson.Address, traffic uint64) (err error) {

	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	for {
		retrieve := accountingPeer.unPayTraffic

		if retrieve.Cmp(big.NewInt(0)) == 0 {
			retrieve, err = a.settlement.RetrieveTraffic(peer)
			if err != nil {
				return err
			}
		}

		ret := big.NewInt(0).Add(retrieve, new(big.Int).SetUint64(traffic))
		available, err := a.settlement.AvailableBalance()
		if err != nil {
			return err
		}

		if available.Cmp(ret) < 0 {
			return ErrLowAvailableExceeded
		}
		if retrieve.Cmp(a.paymentTolerance) < 0 {
			// todo prevent frequent loop logic
			accountingPeer.unPayTraffic = ret
			break
		}

	}

	return nil
}

// Credit increases the amount of credit we have with the given peer
// (and decreases existing debt).
func (a *Accounting) Credit(peer boson.Address, traffic uint64) error {
	a.accountingMu.Lock()
	defer a.accountingMu.Unlock()
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	if err := a.settlement.PutRetrieveTraffic(peer, new(big.Int).SetUint64(traffic)); err != nil {
		a.logger.Errorf("failed to modify retrieve traffic")
		return err
	}
	balance, err := a.settlement.RetrieveTraffic(peer)
	balance = new(big.Int).Add(balance, new(big.Int).SetUint64(traffic))
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
	a.accountingMu.Lock()
	defer a.accountingMu.Unlock()

	tolerance := a.paymentTolerance
	traff, err := a.settlement.TransferTraffic(peer)
	if err != nil {
		return err
	}
	if tolerance.Cmp(traff) <= 0 {
		a.metrics.AccountingDisconnectsCount.Inc()
		a.logger.Errorf("block list %s", peer.String())
		return ErrDisconnectThresholdExceeded
		//return p2p.NewBlockPeerError(10000*time.Hour, ErrDisconnectThresholdExceeded)
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
		a.logger.Errorf("low node traffic balance: %s  -> %s -> %s ", peer.String(), balance, unPaid)
		return fmt.Errorf("low node traffic balance: %s ", peer.String())
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
		peerData = &accountingPeer{
			paymentThreshold: a.paymentThreshold,
			unPayTraffic:     big.NewInt(0),
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
	unPay := accountingPeer.unPayTraffic
	if unPay.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}
	if unPay.Cmp(traffic) < 0 {
		return fmt.Errorf("unpaid traffic is less than paid traffic")
	}
	accountingPeer.unPayTraffic = new(big.Int).Sub(unPay, traffic)
	return nil
}
