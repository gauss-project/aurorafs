// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package netstore provides an abstraction layer over the
// Swarm local storage layer that leverages connectivity
// with other peers in order to retrieve chunks from the network that cannot
// be found locally.
package netstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/logging"

	"github.com/gauss-project/aurorafs/pkg/retrieval"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

type store struct {
	storage.Storer
	retrieval        retrieval.Interface
	logger           logging.Logger
	//recoveryCallback recovery.Callback // this is the callback to be executed when a chunk fails to be retrieved
}

var (
	ErrRecoveryAttempt = errors.New("failed to retrieve chunk, recovery initiated")
)

// New returns a new NetStore that wraps a given Storer.
func New(s storage.Storer, r retrieval.Interface, logger logging.Logger) storage.Storer {
	return &store{Storer: s,retrieval: r, logger: logger}
}

// Get retrieves a given chunk address.
// It will request a chunk from the network whenever it cannot be found locally.
func (s *store) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address, rootCid ...boson.Address) (ch boson.Chunk, err error) {
	ch, err = s.Storer.Get(ctx, mode, addr, rootCid...)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) && len(rootCid) > 0 {
			// request from network
			ch, err = s.retrieval.RetrieveChunk(ctx, rootCid[0], addr)
			if err != nil {
				return nil, storage.ErrNotFound
			}
		}
		return nil, fmt.Errorf("netstore get: %w", err)
	}
	return ch, nil
}
