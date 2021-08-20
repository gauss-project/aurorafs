// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package routetab

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gogf/gf/os/gmlock"
)

var (
	ProtocolName        = protocolName
	ProtocolVersion     = protocolVersion
	StreamFindRouteReq  = streamFindRouteReq
	StreamFindRouteResp = streamFindRouteResp

	CheckExpired      = checkExpired
	ConvRouteToPbRouteList = convRouteToPbRouteList
	MergeRouteList    = mergeRouteList
	NewMetrics        = newMetrics
	NewPendCallResTab = newPendCallResTab
	NewRouteTable     = newRouteTable
	PathToRouteItem   = pathToRouteItem
	UpdateRouteItem   = updateRouteItem
)

func (s *Service) DoReq(ctx context.Context, src boson.Address, peer p2p.Peer, dest boson.Address, req *pb.FindRouteReq, ch chan struct{}) {
	s.doReq(ctx, src, peer, dest, req, ch)
}

func (s *Service) DoResp(ctx context.Context, peer p2p.Peer, dest boson.Address, routes []RouteItem) {
	s.doResp(ctx, peer, dest, routes)
}

func (s *Service) RouteTab() *routeTable {
	return s.routeTable
}

func (s *Service) Address() boson.Address {
	return s.addr
}

func (s *Service) Kad() *kademlia.Kad {
	return s.kad
}

func (s *Service) P2P() p2p.Streamer {
	return s.streamer
}

func (pend *pendCallResTab) GetItems() map[common.Hash]pendingCallResArray {
	return pend.items
}

func (rt *routeTable) TryLock(key string, f func() error, trylock func(string) bool, unlock func(string)) error {
	return rt.tryLock(key, f, trylock, unlock)
}

func (rt *routeTable) Locker() *gmlock.Locker {
	return rt.mu
}