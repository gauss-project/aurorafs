// Package topology exposes abstractions needed in
// topology-aware components.
package topology

import (
	"errors"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
	"io"
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
	SubscribePeerState(state p2p.PeerState) (c <-chan p2p.Peer, unsubscribe func())
	io.Closer
	Halter
	Snapshot() *model.KadParams
	DisconnectForce(addr boson.Address, reason string) error
	Outbound(peer p2p.Peer)
	SnapshotConnected() (connected int, peers map[string]*model.PeerInfo)
	EachKnownPeerer
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
	ClosestPeer(addr boson.Address, includeSelf bool, skipPeers ...boson.Address) (peerAddr boson.Address, err error)
	ClosestPeers(addr boson.Address, limit int, skipPeers ...boson.Address) ([]boson.Address, error)
}

type EachPeerer interface {
	// EachPeer iterates from closest bin to farthest
	EachPeer(model.EachPeerFunc) error
	// EachPeerRev iterates from farthest bin to closest
	EachPeerRev(model.EachPeerFunc) error
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
