// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package blocker_test

import (
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/blocker"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/logging"
)

var (
	flagTime  = time.Millisecond * 25
	checkTime = time.Millisecond * 100
	blockTime = time.Second
	addr      = test.RandomAddress()
	logger    = logging.New(ioutil.Discard, 0)
)

func TestBlocksAfterFlagTimeout(t *testing.T) {
	var (
		mu      sync.Mutex
		blocked = make(map[string]time.Duration)
		mock    = mockBlockLister(func(a boson.Address, d time.Duration, r string) error {
			mu.Lock()
			blocked[a.ByteString()] = d
			mu.Unlock()

			return nil
		})
		b = blocker.New(mock, flagTime, blockTime, time.Millisecond, nil, logger)
	)
	defer b.Close()

	b.Flag(addr)

	mu.Lock()
	if _, ok := blocked[addr.ByteString()]; ok {
		mu.Unlock()
		t.Fatal("blocker did not wait flag duration")
	}
	mu.Unlock()

	midway := time.After(flagTime / 2)
	check := time.After(checkTime)

	<-midway
	b.Flag(addr) // check thats this flag call does not overide previous call
	<-check

	mu.Lock()
	blockedTime, ok := blocked[addr.ByteString()]
	mu.Unlock()
	if !ok {
		t.Fatal("address should be blocked")
	}

	if blockedTime != blockTime {
		t.Fatalf("block time: want %v, got %v", blockTime, blockedTime)
	}

}

func TestUnflagBeforeBlock(t *testing.T) {
	var (
		mu      sync.Mutex
		blocked = make(map[string]time.Duration)
		mock    = mockBlockLister(func(a boson.Address, d time.Duration, r string) error {
			mu.Lock()
			blocked[a.ByteString()] = d
			mu.Unlock()
			return nil
		})
		logger = logging.New(ioutil.Discard, 0)
		b      = blocker.New(mock, flagTime, blockTime, time.Millisecond, nil, logger)
	)
	defer b.Close()
	b.Flag(addr)

	mu.Lock()
	if _, ok := blocked[addr.ByteString()]; ok {
		mu.Unlock()
		t.Fatal("blocker did not wait flag duration")
	}
	mu.Unlock()

	b.Unflag(addr)

	time.Sleep(checkTime)

	mu.Lock()
	_, ok := blocked[addr.ByteString()]
	mu.Unlock()

	if ok {
		t.Fatal("address should not be blocked")
	}

}

func TestPruneBeforeBlock(t *testing.T) {
	var (
		mu      sync.Mutex
		blocked = make(map[string]time.Duration)
		mock    = mockBlockLister(func(a boson.Address, d time.Duration, r string) error {
			mu.Lock()
			blocked[a.ByteString()] = d
			mu.Unlock()
			return nil
		})
		b = blocker.New(mock, flagTime, blockTime, time.Millisecond, nil, logger)
	)
	defer b.Close()

	b.Flag(addr)

	mu.Lock()
	if _, ok := blocked[addr.ByteString()]; ok {
		mu.Unlock()
		t.Fatal("blocker did not wait flag duration")
	}
	mu.Unlock()

	// communicate that we have seen no peers, resulting in the peer being removed
	b.PruneUnseen([]boson.Address{})

	time.Sleep(checkTime)

	mu.Lock()
	_, ok := blocked[addr.ByteString()]
	mu.Unlock()

	if ok {
		t.Fatal("address should not be blocked")
	}

}

type blocklister struct {
	blocklistFunc func(boson.Address, time.Duration, string) error
}

func mockBlockLister(f func(boson.Address, time.Duration, string) error) *blocklister {
	return &blocklister{
		blocklistFunc: f,
	}
}

func (b *blocklister) Blocklist(addr boson.Address, t time.Duration, r string) error {
	return b.blocklistFunc(addr, t, r)
}
