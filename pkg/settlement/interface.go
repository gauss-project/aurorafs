package settlement

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common"
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
	Pay(ctx context.Context, chainAddress common.Address, traffic *big.Int) error
	TransferTraffic(chainAddress common.Address) (traffic *big.Int, err error)
	RetrieveTraffic(chainAddress common.Address) (traffic *big.Int, err error)
	PutRetrieveTraffic(chainAddress common.Address, traffic *big.Int) error
	PutTransferTraffic(chainAddress common.Address, traffic *big.Int) error
	// TotalSent returns the total amount sent to a peer
	TotalSent(chainAddress common.Address) (totalSent *big.Int, err error)
	// TotalReceived returns the total amount received from a peer
	TotalReceived(chainAddress common.Address) (totalReceived *big.Int, err error)
	// SettlementsSent returns sent settlements for each individual known peer
	SettlementsSent() (map[string]*big.Int, error)
	// SettlementsReceived returns received settlements for each individual known peer
	SettlementsReceived() (map[string]*big.Int, error)
	// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
	SetNotifyPaymentFunc(notifyPaymentFunc NotifyPaymentFunc)
}

// NotifyPaymentFunc is called when a payment from peer was successfully received
type NotifyPaymentFunc func(peer boson.Address, amount *big.Int) error
