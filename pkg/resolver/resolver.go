// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resolver

import (
	"io"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

// Address is the boson aurora address.
type Address = boson.Address

// Interface can resolve an URL into an associated Ethereum address.
type Interface interface {
	Resolve(url string) (Address, error)
	io.Closer
}
