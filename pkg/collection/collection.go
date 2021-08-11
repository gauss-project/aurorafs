// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package collection provides high-level abstractions for collections of files
package collection

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
)

// Collection provides a specific ordering of a collection of binary data vectors
// stored in aurorafs.
type Collection interface {
	Addresses() []boson.Address
}

// Entry encapsulates data defining a single file entry.
// It may contain any number of data blobs providing context to the
// given data vector concealed by Reference.
type Entry interface {
	Reference() boson.Address
	Metadata() boson.Address
}
