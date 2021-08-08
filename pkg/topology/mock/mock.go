// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mock

import (
	"context"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/topology"
)

type mock struct {
	peers          []boson.Address
	closestPeer    boson.Address
	closestPeerErr error
	addPeersErr     error
	marshalJSONFunc func() ([]byte, error)
	mtx             sync.Mutex
}

func WithPeers(peers ...boson.Address) Option {
	return optionFunc(func(d *mock) {
		d.peers = peers
	})
}

func WithAddPeersErr(err error) Option {
	return optionFunc(func(d *mock) {
		d.addPeersErr = err
	})
}

func WithClosestPeer(addr boson.Address) Option {
	return optionFunc(func(d *mock) {
		d.closestPeer = addr
	})
}

func WithClosestPeerErr(err error) Option {
	return optionFunc(func(d *mock) {
		d.closestPeerErr = err
	})
}

func WithMarshalJSONFunc(f func() ([]byte, error)) Option {
	return optionFunc(func(d *mock) {
		d.marshalJSONFunc = f
	})
}

func NewTopologyDriver(opts ...Option) topology.Driver {
	d := new(mock)
	for _, o := range opts {
		o.apply(d)
	}
	return d
}

func (d *mock) AddPeers(_ context.Context, addrs ...boson.Address) error {
	if d.addPeersErr != nil {
		return d.addPeersErr
	}

	for _, addr := range addrs {
		d.mtx.Lock()
		d.peers = append(d.peers, addr)
		d.mtx.Unlock()
	}

	return nil
}

func (d *mock) Connected(ctx context.Context, addr boson.Address) error {
	return d.AddPeers(ctx, addr)
}

func (d *mock) Disconnected(boson.Address) {
	panic("todo")
}

func (d *mock) Peers() []boson.Address {
	return d.peers
}

func (d *mock) ClosestPeer(_ boson.Address, skipPeers ...boson.Address) (peerAddr boson.Address, err error) {
	if len(skipPeers) == 0 {
		if d.closestPeerErr != nil {
			return d.closestPeer, d.closestPeerErr
		}
		if !d.closestPeer.Equal(boson.ZeroAddress) {
			return d.closestPeer, nil
		}
	}

	d.mtx.Lock()
	defer d.mtx.Unlock()

	skipPeer := false
	for _, p := range d.peers {
		for _, a := range skipPeers {
			if a.Equal(p) {
				skipPeer = true
				break
			}
		}
		if skipPeer {
			skipPeer = false
			continue
		}

		peerAddr = p
	}

	if peerAddr.IsZero() {
		return peerAddr, topology.ErrNotFound
	}
	return peerAddr, nil
}

func (d *mock) SubscribePeersChange() (c <-chan struct{}, unsubscribe func()) {
	return c, unsubscribe
}

func (*mock) NeighborhoodDepth() uint8 {
	return 0
}

// EachPeer iterates from closest bin to farthest
func (d *mock) EachPeer(f topology.EachPeerFunc) (err error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	for i, p := range d.peers {
		_, _, err = f(p, uint8(i))
		if err != nil {
			return
		}
	}

	return nil
}

// EachPeerRev iterates from farthest bin to closest
func (d *mock) EachPeerRev(f topology.EachPeerFunc) (err error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	for i := len(d.peers) - 1; i >= 0; i-- {
		_, _, err = f(d.peers[i], uint8(i))
		if err != nil {
			return
		}
	}

	return nil
}

func (d *mock) MarshalJSON() ([]byte, error) {
	return d.marshalJSONFunc()
}

func (d *mock) Close() error {
	return nil
}

type Option interface {
	apply(*mock)
}

type optionFunc func(*mock)

func (f optionFunc) apply(r *mock) { f(r) }
