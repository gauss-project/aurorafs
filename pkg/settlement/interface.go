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
	Pay(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error

	TransferTraffic(peer boson.Address) (traffic *big.Int, err error)

	RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error)

	PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error

	PutTransferTraffic(peer boson.Address, traffic *big.Int) error

	// AvailableBalance Get actual available balance
	AvailableBalance() (*big.Int, error)

	// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
	SetNotifyPaymentFunc(notifyPaymentFunc NotifyPaymentFunc)

	GetPeerBalance(peer boson.Address) (*big.Int, error)

	GetUnPaidBalance(peer boson.Address) (*big.Int, error)
}

// NotifyPaymentFunc is called when a payment from peer was successfully received
type NotifyPaymentFunc func(peer boson.Address, amount *big.Int) error
