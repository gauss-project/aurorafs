// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package routetab

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/topology"
)

var (
	ProtocolName        = protocolName
	ProtocolVersion     = protocolVersion
	StreamFindRouteReq  = streamOnRouteReq
	StreamFindRouteResp = streamOnRouteResp

	CheckExpired      = checkExpired
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

func (s *Service) Kad() topology.Driver {
	return s.topology
}

func (s *Service) P2P() p2p.Streamer {
	return s.streamer
}

func (pend *pendCallResTab) GetItems() map[common.Hash]pendingCallResArray {
	return pend.items
}
