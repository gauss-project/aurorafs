// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package traffic

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"sync"

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
	//peer:beneficiary
	peerBeneficiary sync.Map
	//beneficiary:peer
	beneficiaryPeer sync.Map
}

// NewAddressbook creates a new addressbook using the store.
func NewAddressBook(store storage.StateStorer) Addressbook {
	return &addressbook{
		store: store,
	}
}

func (a *addressbook) initAddressBook() error {
	//if err := a.store.Iterate(beneficiaryPeerPrefix, func(k, value []byte) (stop bool, err error) {
	//	if !strings.HasPrefix(string(k), beneficiaryPeerPrefix) {
	//		return true, nil
	//	}
	//
	//	key := string(k)
	//	rootCid, _, err := unmarshalKey(pyramidKeyPrefix, key)
	//	if err != nil {
	//		return false, err
	//	}
	//
	//	var overlay string
	//	if err = a.store.Get(key, &overlay); err != nil {
	//		return false, err
	//	}
	//
	//	cs.putPyramidSource(rootCid, overlay)
	//	return false, nil
	//}); err != nil {
	//	return err
	//}
	return nil
}
func (a *addressbook) putPeerBeneficiary(peer boson.Address, beneficiary common.Address) error {
	a.peerBeneficiary.Store(peer.String(), beneficiary)
	return nil
}

// Beneficiary returns the beneficiary for the given peer.
func (a *addressbook) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	if value, ok := a.peerBeneficiary.Load(peer.String()); ok {
		return value.(common.Address), true, nil
	} else {
		return common.Address{}, false, storage.ErrNotFound
	}
}

// BeneficiaryPeer returns the peer for a beneficiary.
func (a *addressbook) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	if value, ok := a.beneficiaryPeer.Load(beneficiary.String()); ok {
		return value.(boson.Address), true, nil
	} else {
		return boson.Address{}, false, storage.ErrNotFound
	}
}

// PutBeneficiary stores the beneficiary for the given peer.
func (a *addressbook) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	err := a.putPeerBeneficiary(peer, beneficiary)
	if err != nil {
		return err
	}
	a.beneficiaryPeer.Store(beneficiary.String(), peer)

	err = a.store.Put(peerBeneficiaryKey(peer), beneficiary)
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
