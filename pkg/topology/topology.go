// Package topology exposes abstractions needed in
// topology-aware components.
package topology

import (
	"errors"
	"io"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
)

var (
	ErrNotFound      = errors.New("no peer found")
	ErrWantSelf      = errors.New("node wants self")
	ErrOversaturated = errors.New("oversaturated")
)

type Driver interface {
	p2p.Notifier
	PeerAdder
	ClosestPeerer
	EachPeerer
	EachNeighbor
	NeighborhoodDepther
	SubscribePeersChange() (c <-chan struct{}, unsubscribe func())
	SubscribePeerState() (c <-chan p2p.PeerInfo, unsubscribe func())
	io.Closer
	Halter
	Snapshot() *model.KadParams
	SnapshotConnected() (connected int, peers map[string]*model.PeerInfo)
	SnapshotAddr(addr boson.Address) *model.Snapshot
	GetPeersWithLatencyEWMA(list []boson.Address) (now []boson.Address)
	RecordPeerLatency(add boson.Address, t time.Duration)
	DisconnectForce(addr boson.Address, reason string) error
	Outbound(peer p2p.Peer)
	NotifyPeerState(peer p2p.PeerInfo)
	EachKnownPeerer
	ProtectPeer
}

type ProtectPeer interface {
	RefreshProtectPeer(peer []boson.Address)
}

type PeerAdder interface {
	// AddPeers is called when peers are added to the topology backlog
	AddPeers(addr ...boson.Address)
}

type ClosestPeerer interface {
	// ClosestPeer returns the closest connected peer we have in relation to a
	// given chunk address.
	// This function will ignore peers with addresses provided in skipPeers.
	// Returns topology.ErrWantSelf in case base is the closest to the address.
	ClosestPeer(addr boson.Address, includeSelf bool, f Filter, skipPeers ...boson.Address) (peerAddr boson.Address, err error)
	ClosestPeers(addr boson.Address, limit int, f Filter, skipPeers ...boson.Address) ([]boson.Address, error)
}

type EachPeerer interface {
	// EachPeer iterates from closest bin to farthest
	EachPeer(model.EachPeerFunc, Filter) error
	// EachPeerRev iterates from farthest bin to closest
	EachPeerRev(model.EachPeerFunc, Filter) error
}

// Filter defines the different filters that can be used with the Peer iterators
type Filter struct {
	Reachable bool
}

type EachKnownPeerer interface {
	// EachKnownPeer iterates from closest bin to farthest
	EachKnownPeer(model.EachPeerFunc) error
	// EachKnownPeerRev iterates from farthest bin to closest
	EachKnownPeerRev(model.EachPeerFunc) error
}

type EachNeighbor interface {
	// EachNeighbor iterates from closest bin to farthest within the neighborhood.
	EachNeighbor(model.EachPeerFunc) error
	// EachNeighborRev iterates from farthest bin to closest within the neighborhood.
	EachNeighborRev(model.EachPeerFunc) error
	// IsWithinDepth checks if an address is the within neighborhood.
	IsWithinDepth(boson.Address) bool
}

type Halter interface {
	// Halt the topology from initiating new connections
	// while allowing it to still run.
	Halt()
}

type NeighborhoodDepther interface {
	NeighborhoodDepth() uint8
}
