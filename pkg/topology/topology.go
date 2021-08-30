// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package topology exposes abstractions needed in
// topology-aware components.
package topology

import (
	"errors"
	"io"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	ErrNotFound      = errors.New("no peer found")
	ErrWantSelf      = errors.New("node wants self")
	ErrOversaturated = errors.New("oversaturated")
)

type Driver interface {
	PeerAdder
	ClosestPeerer
	EachPeerer
	NeighborhoodDepth() uint8
	SubscribePeersChange() (c <-chan struct{}, unsubscribe func())
	io.Closer
}

type PeerAdder interface {
	// AddPeers is called when peers are added to the topology backlog
	AddPeers(...boson.Address) error
}

type ClosestPeerer interface {
	// ClosestPeer returns the closest connected peer we have in relation to a
	// given chunk address.
	// This function will ignore peers with addresses provided in skipPeers.
	// Returns topology.ErrWantSelf in case base is the closest to the address.
	ClosestPeer(addr boson.Address, skipPeers ...boson.Address) (peerAddr boson.Address, err error)
}

type EachPeerer interface {
	// EachPeer iterates from closest bin to farthest
	EachPeer(EachPeerFunc) error
	// EachPeerRev iterates from farthest bin to closest
	EachPeerRev(EachPeerFunc) error
}

// EachPeerFunc is a callback that is called with a peer and its PO
type EachPeerFunc func(boson.Address, uint8) (stop, jumpToNext bool, err error)
