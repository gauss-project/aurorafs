// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval

import (
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

type skipPeers struct {
	addresses []boson.Address
	mu        sync.Mutex
}

func newSkipPeers() *skipPeers {
	return &skipPeers{}
}

func (s *skipPeers) All() []boson.Address {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append(s.addresses[:0:0], s.addresses...)
}

func (s *skipPeers) Add(address boson.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, a := range s.addresses {
		if a.Equal(address) {
			return
		}
	}

	s.addresses = append(s.addresses, address)
}
