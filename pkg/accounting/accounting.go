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
	"strings"
	"sync"
)

var (
	_                     Interface = (*Accounting)(nil)
	balancesPrefix        string    = "accounting_balance_"
	balancesSurplusPrefix string    = "accounting_surplusbalance_"
)

// Interface is the Accounting interface.
type Interface interface {
	// Reserve reserves a portion of the balance for peer and attempts settlements if necessary.
	// Returns an error if the operation risks exceeding the disconnect threshold or an attempted settlement failed.
	//
	// This has to be called (always in combination with Release) before a
	// Credit action to prevent overspending in case of concurrent requests.
	Reserve(ctx context.Context, peer boson.Address, traffic uint64) error
	// Credit increases the balance the peer has with us (we "pay" the peer).
	Credit(peer boson.Address, traffic uint64) error
	// Debit increases the balance we have with the peer (we get "paid" back).
	Debit(peer boson.Address, traffic uint64) error

	RetrievedTraffic(peer boson.Address) (*big.Int, error)

	TransferredTraffic(peer boson.Address) (*big.Int, error)
}

// Accounting is the main implementation of the accounting interface.
type Accounting struct {
	// Mutex for accessing the accountingPeers map.
	accountingPeersMu sync.Mutex
	accountingPeers   map[string]*big.Int
	logger            logging.Logger
	store             storage.StateStorer
	paymentTolerance  *big.Int
	paymentThreshold  *big.Int
	settlement        settlement.Interface
	metrics           metrics
}

var (
	// ErrOverdraft denotes the expected debt in Reserve would exceed the payment thresholds.
	ErrOverdraft = errors.New("attempted overdraft")
	// ErrDisconnectThresholdExceeded denotes a peer has exceeded the disconnect threshold.
	ErrDisconnectThresholdExceeded = errors.New("disconnect threshold exceeded")
	// ErrPeerNoBalance is the error returned if no balance in store exists for a peer
	ErrPeerNoBalance = errors.New("no balance for peer")
	// ErrOverflow denotes an arithmetic operation overflowed.
	ErrOverflow = errors.New("overflow error")
	// ErrInvalidValue denotes an invalid value read from store
	ErrInvalidValue = errors.New("invalid value")
)

// NewAccounting creates a new Accounting instance with the provided options.
func NewAccounting(
	paymentTolerance,
	paymentThreshold *big.Int,
	logger logging.Logger,
	store storage.StateStorer,
	settlement settlement.Interface,
) *Accounting {
	return &Accounting{
		accountingPeers:  make(map[string]*big.Int),
		paymentTolerance: new(big.Int).Set(paymentTolerance),
		paymentThreshold: new(big.Int).Set(paymentThreshold),
		logger:           logger,
		store:            store,
		settlement:       settlement,
		metrics:          newMetrics(),
	}
}

// Reserve reserves a portion of the balance for peer and attempts settlements if necessary.
func (a *Accounting) Reserve(ctx context.Context, peer boson.Address, traffic uint64) error {
	accountingPeer, err := a.getAccountingPeer(peer)
	if err != nil {
		return err
	}
	balance, err := a.settlement.RetrieveTraffic(peer)
	balance = balance.Add(balance, new(big.Int).SetUint64(traffic))
	if balance.Cmp(accountingPeer) < 0 {
		err := a.settle(ctx, peer, balance)
		if err != nil {
			return fmt.Errorf("failed to settle with peer %v: %v", peer, err)
		}
	}
	return nil
}

// Release releases reserved funds.
func (a *Accounting) Release(peer boson.Address, traffic uint64) {
	if err := a.settlement.PutRetrieveTraffic(peer, new(big.Int).SetUint64(traffic)); err != nil {
		a.logger.Errorf("")
		return
	}
}

// Credit increases the amount of credit we have with the given peer
// (and decreases existing debt).
func (a *Accounting) Credit(peer boson.Address, traffic uint64) error {

	return nil
}

// Settle all debt with a peer. The lock on the accountingPeer must be held when
// called.
func (a *Accounting) settle(ctx context.Context, peer boson.Address, balance *big.Int) error {
	if err := a.settlement.Pay(ctx, peer, balance); err != nil {
		return err
	}
	return nil
}

// Debit increases the amount of debt we have with the given peer (and decreases
// existing credit).
func (a *Accounting) Debit(peer boson.Address, traffic uint64) error {

	return nil
}

func (a *Accounting) RetrievedTraffic(peer boson.Address) (*big.Int, error) {
	return new(big.Int).SetInt64(0), nil
}

func (a *Accounting) TransferredTraffic(peer boson.Address) (*big.Int, error) {
	return new(big.Int).SetInt64(0), nil
}

// peerBalanceKey returns the balance storage key for the given peer.
func peerBalanceKey(peer boson.Address) string {
	return fmt.Sprintf("%s%s", balancesPrefix, peer.String())
}

// getAccountingPeer returns the accountingPeer for a given boson address.
// If not found in memory it will initialize it.
func (a *Accounting) getAccountingPeer(peer boson.Address) (*big.Int, error) {
	a.accountingPeersMu.Lock()
	defer a.accountingPeersMu.Unlock()
	peerData, ok := a.accountingPeers[peer.String()]
	if !ok {
		a.accountingPeers[peer.String()] = a.paymentThreshold
	}
	return peerData, nil
}

// balanceKeyPeer returns the embedded peer from the balance storage key.
func balanceKeyPeer(key []byte) (boson.Address, error) {
	k := string(key)

	split := strings.SplitAfter(k, balancesPrefix)
	if len(split) != 2 {
		return boson.ZeroAddress, errors.New("no peer in key")
	}

	addr, err := boson.ParseHexAddress(split[1])
	if err != nil {
		return boson.ZeroAddress, err
	}

	return addr, nil
}

func surplusBalanceKeyPeer(key []byte) (boson.Address, error) {
	k := string(key)

	split := strings.SplitAfter(k, balancesSurplusPrefix)
	if len(split) != 2 {
		return boson.ZeroAddress, errors.New("no peer in key")
	}

	addr, err := boson.ParseHexAddress(split[1])
	if err != nil {
		return boson.ZeroAddress, err
	}

	return addr, nil
}

// NotifyPayment is called by Settlement when we receive a payment.
func (a *Accounting) NotifyPayment(peer boson.Address, amount *big.Int) error {
	return nil
}

// AsyncNotifyPayment calls notify payment in a go routine.
// This is needed when accounting needs to be notified but the accounting lock is already held.
func (a *Accounting) AsyncNotifyPayment(peer boson.Address, amount *big.Int) error {
	go func() {
		err := a.NotifyPayment(peer, amount)
		if err != nil {
			a.logger.Errorf("failed to notify accounting of payment: %v", err)
		}
	}()
	return nil
}
