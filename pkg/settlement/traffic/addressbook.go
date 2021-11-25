// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package traffic

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

var (
	beneficiaryPeerPrefix = "boson_beneficiary_peer_"
	peerBeneficiaryPrefix = "boson_peer_beneficiary_"
)

// Addressbook maps peers to beneficaries, chequebooks and in reverse.
type Addressbook interface {
	// Beneficiary returns the beneficiary for the given peer.
	Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error)
	// BeneficiaryPeer returns the peer for a beneficiary.
	BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error)
	// PutBeneficiary stores the beneficiary for the given peer.
	PutBeneficiary(peer boson.Address, beneficiary common.Address) error
}

type addressbook struct {
	store storage.StateStorer
}

// NewAddressbook creates a new addressbook using the store.
func NewAddressbook(store storage.StateStorer) Addressbook {
	return &addressbook{
		store: store,
	}
}

// Beneficiary returns the beneficiary for the given peer.
func (a *addressbook) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	err = a.store.Get(peerBeneficiaryKey(peer), &beneficiary)
	if err != nil {
		if err != storage.ErrNotFound {
			return common.Address{}, false, err
		}
		return common.Address{}, false, nil
	}
	return beneficiary, true, nil
}

// BeneficiaryPeer returns the peer for a beneficiary.
func (a *addressbook) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	err = a.store.Get(beneficiaryPeerKey(beneficiary), &peer)
	if err != nil {
		if err != storage.ErrNotFound {
			return boson.Address{}, false, err
		}
		return boson.Address{}, false, nil
	}
	return peer, true, nil
}

// PutBeneficiary stores the beneficiary for the given peer.
func (a *addressbook) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	err := a.store.Put(peerBeneficiaryKey(peer), beneficiary)
	if err != nil {
		return err
	}
	return a.store.Put(beneficiaryPeerKey(beneficiary), peer)
}

// peerBeneficiaryKey computes the key where to store the beneficiary for a peer.
func peerBeneficiaryKey(peer boson.Address) string {
	return fmt.Sprintf("%s%s", peerBeneficiaryPrefix, peer)
}

// beneficiaryPeerKey computes the key where to store the peer for a beneficiary.
func beneficiaryPeerKey(peer common.Address) string {
	return fmt.Sprintf("%s%s", beneficiaryPeerPrefix, peer)
}
