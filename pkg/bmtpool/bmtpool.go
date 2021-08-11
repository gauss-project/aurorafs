// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bmtpool provides easy access to binary
// merkle tree hashers managed in as a resource pool.
package bmtpool

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	bmtlegacy "github.com/ethersphere/bmt/legacy"
	"github.com/ethersphere/bmt/pool"
)

var instance pool.Pooler

func init() {
	instance = pool.New(8, boson.BigBmtBranches)
}

// Get a bmt Hasher instance.
// Instances are reset before being returned to the caller.
func Get() *bmtlegacy.Hasher {
	return instance.Get()
}

// Put a bmt Hasher back into the pool
func Put(h *bmtlegacy.Hasher) {
	instance.Put(h)
}
