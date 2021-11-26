package settlement

import (
	"context"
	"errors"
	"math/big"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	ErrPeerNoSettlements = errors.New("no settlements for peer")
)

// Interface is the interface used by Accounting to trigger settlement
type Interface interface {
	// Pay initiates a payment to the given peer
	// It should return without error it is likely that the payment worked
	Pay(ctx context.Context, peer boson.Address, traffic, paymentThreshold *big.Int) error
	TransferTraffic(peer boson.Address) (traffic *big.Int, err error)
	RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error)
	PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error
	PutTransferTraffic(peer boson.Address, traffic *big.Int) error
	// TotalSent returns the total amount sent to a peer
	TotalSent(peer boson.Address) (totalSent *big.Int, err error)
	// TotalReceived returns the total amount received from a peer
	TotalReceived(peer boson.Address) (totalReceived *big.Int, err error)
	// SettlementsSent returns sent settlements for each individual known peer
	SettlementsSent() (map[string]*big.Int, error)
	// SettlementsReceived returns received settlements for each individual known peer
	SettlementsReceived() (map[string]*big.Int, error)
	// Balance Get chain balance
	Balance(ctx context.Context) (*big.Int, error)
	// AvailableBalance Get actual available balance
	AvailableBalance(ctx context.Context) (*big.Int, error)

	UpdatePeerBalance(peer boson.Address) error
	// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
	SetNotifyPaymentFunc(notifyPaymentFunc NotifyPaymentFunc)
}

// NotifyPaymentFunc is called when a payment from peer was successfully received
type NotifyPaymentFunc func(peer boson.Address, amount *big.Int) error
