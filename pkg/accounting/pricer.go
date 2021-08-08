// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package accounting

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
)

// Pricer returns pricing information for chunk hashes.
type Pricer interface {
	// PeerPrice is the price the peer charges for a given chunk hash.
	PeerPrice(peer, chunk boson.Address) uint64
	// Price is the price we charge for a given chunk hash.
	Price(chunk boson.Address) uint64
}

// FixedPricer is a Pricer that has a fixed price for chunks.
type FixedPricer struct {
	overlay boson.Address
	poPrice uint64
}

// NewFixedPricer returns a new FixedPricer with a given price.
func NewFixedPricer(overlay boson.Address, poPrice uint64) *FixedPricer {
	return &FixedPricer{
		overlay: overlay,
		poPrice: poPrice,
	}
}

// PeerPrice implements Pricer.
func (pricer *FixedPricer) PeerPrice(peer, chunk boson.Address) uint64 {
	return uint64(boson.MaxPO-boson.Proximity(peer.Bytes(), chunk.Bytes())+1) * pricer.poPrice
}

// Price implements Pricer.
func (pricer *FixedPricer) Price(chunk boson.Address) uint64 {
	return pricer.PeerPrice(pricer.overlay, chunk)
}
