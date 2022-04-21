// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pinning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/hashicorp/go-multierror"
)

// ErrTraversal signals that errors occurred during nodes traversal.
var ErrTraversal = errors.New("traversal iteration failed")

// Interface defines pinning operations.
type Interface interface {
	// CreatePin creates a new pin for the given reference.
	// The boolean arguments specifies whether all nodes
	// in the tree should also be traversed and pinned.
	// Repeating calls of this method are idempotent.
	CreatePin(context.Context, boson.Address, bool) error
	// DeletePin deletes given reference. All the existing
	// nodes in the tree will also be traversed and un-pinned.
	// Repeating calls of this method are idempotent.
	DeletePin(context.Context, boson.Address) error
	// HasPin returns true if the given reference has root pin.
	HasPin(boson.Address) (bool, error)
	// Pins return all pinned references.
	Pins() ([]boson.Address, error)
}

const storePrefix = "root-pin"

func rootPinKey(ref boson.Address) string {
	return fmt.Sprintf("%s-%s", storePrefix, ref)
}

// NewService is a convenient constructor for Service.
func NewService(
	pinStorage storage.Storer,
	rhStorage storage.StateStorer,
	traverser traversal.Traverser,
) *Service {
	return &Service{
		pinStorage: pinStorage,
		rhStorage:  rhStorage,
		traverser:  traverser,
	}
}

// Service is implementation of the pinning.Interface.
type Service struct {
	pinStorage storage.Storer
	rhStorage  storage.StateStorer
	traverser  traversal.Traverser
}

// CreatePin implements Interface.CreatePin method.
func (s *Service) CreatePin(ctx context.Context, ref boson.Address, traverse bool) error {
	// iterFn is a pinning iterator function over the leaves of the root.
	ctx = sctx.SetRootHash(ctx, ref)
	iterFn := func(leaf boson.Address) error {
		switch err := s.pinStorage.Set(ctx, storage.ModeSetPin, leaf); {
		case errors.Is(err, storage.ErrNotFound):
			// ignore
		case err != nil:
			return fmt.Errorf("unable to set pin for leaf %q of root %q: %w", leaf, ref, err)
		}
		return nil
	}

	if traverse {
		if err := s.traverser.Traverse(ctx, ref, iterFn); err != nil {
			return fmt.Errorf("traversal of %q failed: %w", ref, err)
		}
	}

	key := rootPinKey(ref)
	switch err := s.rhStorage.Get(key, new(boson.Address)); {
	case errors.Is(err, storage.ErrNotFound):
		return s.rhStorage.Put(key, ref)
	case err != nil:
		return fmt.Errorf("unable to pin %q: %w", ref, err)
	}
	return nil
}

// DeletePin implements Interface.DeletePin method.
func (s *Service) DeletePin(ctx context.Context, ref boson.Address) error {
	var iterErr error
	ctx = sctx.SetRootHash(ctx, ref)
	// iterFn is a unpinning iterator function over the leaves of the root.
	iterFn := func(leaf boson.Address) error {
		err := s.pinStorage.Set(ctx, storage.ModeSetUnpin, leaf)
		if err != nil {
			iterErr = multierror.Append(err, fmt.Errorf("unable to unpin the chunk for leaf %q of root %q: %w", leaf, ref, err))
			// Continue un-pinning all chunks.
		}
		return nil
	}

	if err := s.traverser.Traverse(ctx, ref, iterFn); err != nil {
		return fmt.Errorf("traversal of %q failed: %w", ref, multierror.Append(err, iterErr))
	}
	if iterErr != nil {
		return multierror.Append(ErrTraversal, iterErr)
	}

	key := rootPinKey(ref)
	if err := s.rhStorage.Delete(key); err != nil {
		return fmt.Errorf("unable to delete pin for key %q: %w", key, err)
	}
	return nil
}

// HasPin implements Interface.HasPin method.
func (s *Service) HasPin(ref boson.Address) (bool, error) {
	key, val := rootPinKey(ref), boson.NewAddress(nil)
	switch err := s.rhStorage.Get(key, &val); {
	case errors.Is(err, storage.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("unable to get pin for key %q: %w", key, err)
	}
	return val.Equal(ref), nil
}

// Pins implements Interface.Pins method.
func (s *Service) Pins() ([]boson.Address, error) {
	var refs []boson.Address
	err := s.rhStorage.Iterate(storePrefix, func(key, val []byte) (stop bool, err error) {
		var ref boson.Address
		if err := json.Unmarshal(val, &ref); err != nil {
			return true, fmt.Errorf("invalid reference value %q: %w", string(val), err)
		}
		refs = append(refs, ref)
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("iteration failed: %w", err)
	}
	return refs, nil
}
