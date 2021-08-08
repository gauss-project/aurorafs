// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package discovery exposes the discovery driver interface
// which is implemented by discovery protocols.
package discovery

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

type Driver interface {
	BroadcastPeers(ctx context.Context, addressee boson.Address, peers ...boson.Address) error
}
