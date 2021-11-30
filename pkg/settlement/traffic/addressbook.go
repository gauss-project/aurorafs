// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package traffic

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"strings"
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
	Beneficiary(peer boson.Address) (beneficiary common.Address, known bool)
	// BeneficiaryPeer returns the peer for a beneficiary.
	BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error)
	// PutBeneficiary stores the beneficiary for the given peer.
	PutBeneficiary(peer boson.Address, beneficiary common.Address) error

	InitAddressBook() error
}

type addressBook struct {
	store storage.StateStorer
	//peer:beneficiary
	peerBeneficiary sync.Map
	//beneficiary:peer
	beneficiaryPeer sync.Map
}

// NewAddressbook creates a new addressbook using the store.
func NewAddressBook(store storage.StateStorer) Addressbook {
	return &addressBook{
		store: store,
	}
}

func (a *addressBook) InitAddressBook() error {
	if err := a.store.Iterate(peerBeneficiaryPrefix, func(k, value []byte) (stop bool, err error) {
		if !strings.HasPrefix(string(k), peerBeneficiaryPrefix) {
			return true, nil
		}

		key := string(k)
		peer, err := peerUnmarshalKey(peerBeneficiaryPrefix, key)
		if err != nil {
			return false, err
		}

		var beneficiary common.Address
		if err = a.store.Get(key, &beneficiary); err != nil {
			return true, err
		}

		if err := a.putPeerBeneficiary(peer, beneficiary); err != nil {
			return true, err
		}

		return false, nil
	}); err != nil {
		return err
	}

	if err := a.store.Iterate(beneficiaryPeerPrefix, func(k, value []byte) (stop bool, err error) {
		if !strings.HasPrefix(string(k), beneficiaryPeerPrefix) {
			return true, nil
		}

		key := string(k)
		beneficiary, err := beneficiaryUnmarshalKey(beneficiaryPeerPrefix, key)
		if err != nil {
			return false, err
		}

		var peer boson.Address
		if err = a.store.Get(key, &peer); err != nil {
			return true, err
		}

		if err := a.putBeneficiaryPeer(peer, beneficiary); err != nil {
			return true, err
		}

		return false, nil
	}); err != nil {
		return err
	}

	return nil
}
func (a *addressBook) putPeerBeneficiary(peer boson.Address, beneficiary common.Address) error {
	a.peerBeneficiary.Store(peer.String(), beneficiary)
	return nil
}

func (a *addressBook) putBeneficiaryPeer(peer boson.Address, beneficiary common.Address) error {
	a.beneficiaryPeer.Store(beneficiary.String(), peer)
	return nil
}

// Beneficiary returns the beneficiary for the given peer.
func (a *addressBook) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool) {
	if value, ok := a.peerBeneficiary.Load(peer.String()); ok {
		return value.(common.Address), true
	} else {
		return common.Address{}, false
	}
}

// BeneficiaryPeer returns the peer for a beneficiary.
func (a *addressBook) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	if value, ok := a.beneficiaryPeer.Load(beneficiary.String()); ok {
		return value.(boson.Address), true, nil
	} else {
		return boson.Address{}, false, storage.ErrNotFound
	}
}

// PutBeneficiary stores the beneficiary for the given peer.
func (a *addressBook) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	err := a.putPeerBeneficiary(peer, beneficiary)
	if err != nil {
		return err
	}
	err = a.putBeneficiaryPeer(peer, beneficiary)
	if err != nil {
		return err
	}

	err = a.store.Put(peerBeneficiaryKey(peer), beneficiary)
	if err != nil {
		return err
	}
	return a.store.Put(beneficiaryPeerKey(beneficiary), peer)
}

// peerBeneficiaryKey computes the key where to store the beneficiary for a peer.
func peerBeneficiaryKey(peer boson.Address) string {
	return fmt.Sprintf("%s-%s", peerBeneficiaryPrefix, peer)
}

// beneficiaryPeerKey computes the key where to store the peer for a beneficiary.
func beneficiaryPeerKey(peer common.Address) string {
	return fmt.Sprintf("%s-%s", beneficiaryPeerPrefix, peer)
}

func peerUnmarshalKey(keyPrefix, key string) (boson.Address, error) {
	addr := strings.TrimPrefix(key, keyPrefix)
	keys := strings.Split(addr, "-")

	overlay, err := boson.ParseHexAddress(keys[1])
	if err != nil {
		return boson.ZeroAddress, err
	}
	return overlay, nil
}

func beneficiaryUnmarshalKey(keyPrefix, key string) (common.Address, error) {
	addr := strings.TrimPrefix(key, keyPrefix)
	keys := strings.Split(addr, "-")

	if len(keys) > 1 {
		return common.HexToAddress(keys[1]), nil
	} else {
		return common.BytesToAddress([]byte("")), nil
	}
}
