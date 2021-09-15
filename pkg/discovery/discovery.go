// Package discovery exposes the discovery driver interface
// which is implemented by discovery protocols.
package discovery

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

type Driver interface {
	// BroadcastPeers hive implement
	BroadcastPeers(ctx context.Context, addressee boson.Address, peers ...boson.Address) error

	// DoFindNode hive2 implement
	DoFindNode(ctx context.Context, target, peer boson.Address, pos []int32, limit int32) (res chan boson.Address, err error)

	// IsStart entry to run
	IsStart() bool

	// IsHive2 hive2 protocol
	IsHive2() bool
}
