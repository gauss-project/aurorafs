// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mock

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/pinning"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

var _ pinning.Interface = (*ServiceMock)(nil)

// NewServiceMock is a convenient constructor for creating ServiceMock.
func NewServiceMock() *ServiceMock {
	return &ServiceMock{index: make(map[string]int)}
}

// ServiceMock represents a simple mock of pinning.Interface.
// The implementation is not goroutine-safe.
type ServiceMock struct {
	index      map[string]int
	references []boson.Address
}

// CreatePin implements pinning.Interface CreatePin method.
func (sm *ServiceMock) CreatePin(_ context.Context, ref boson.Address, _ bool) error {
	if _, ok := sm.index[ref.String()]; ok {
		return nil
	}
	sm.index[ref.String()] = len(sm.references)
	sm.references = append(sm.references, ref)
	return nil
}

// DeletePin implements pinning.Interface DeletePin method.
func (sm *ServiceMock) DeletePin(_ context.Context, ref boson.Address) error {
	i, ok := sm.index[ref.String()]
	if !ok {
		return nil
	}
	delete(sm.index, ref.String())
	sm.references = append(sm.references[:i], sm.references[i+1:]...)
	return nil
}

// HasPin implements pinning.Interface HasPin method.
func (sm *ServiceMock) HasPin(ref boson.Address) (bool, error) {
	_, ok := sm.index[ref.String()]
	return ok, nil
}

// Pins implements pinning.Interface Pins method.
func (sm *ServiceMock) Pins() ([]boson.Address, error) {
	return append([]boson.Address(nil), sm.references...), nil
}