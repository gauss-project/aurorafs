// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testing

import (
	"testing"

	"github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/soc"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

// MockSOC defines a mocked SOC with exported fields for easy testing.
type MockSOC struct {
	ID           soc.ID
	Owner        []byte
	Signature    []byte
	WrappedChunk boson.Chunk
}

// Address returns the SOC address of the mocked SOC.
func (ms MockSOC) Address() boson.Address {
	addr, _ := soc.CreateAddress(ms.ID, ms.Owner)
	return addr
}

// Chunk returns the SOC chunk of the mocked SOC.
func (ms MockSOC) Chunk() boson.Chunk {
	return boson.NewChunk(ms.Address(), append(ms.ID, append(ms.Signature, ms.WrappedChunk.Data()...)...))
}

// GenerateMockSOC generates a valid mocked SOC from given data.
func GenerateMockSOC(t *testing.T, data []byte) *MockSOC {
	t.Helper()

	privKey, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.NewDefaultSigner(privKey)
	owner, err := signer.EthereumAddress()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cac.New(data)
	if err != nil {
		t.Fatal(err)
	}

	id := make([]byte, soc.IdSize)
	hasher := boson.NewHasher()
	_, err = hasher.Write(append(id, ch.Address().Bytes()...))
	if err != nil {
		t.Fatal(err)
	}

	signature, err := signer.Sign(hasher.Sum(nil))
	if err != nil {
		t.Fatal(err)
	}

	return &MockSOC{
		ID:           id,
		Owner:        owner.Bytes(),
		Signature:    signature,
		WrappedChunk: ch,
	}
}
