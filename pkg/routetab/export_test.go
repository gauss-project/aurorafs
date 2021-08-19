// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package routetab

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
)

var (
	MergeRouteList  = mergeRouteList
	PathToRouteItem = pathToRouteItem
	NewRouteTable   = newRouteTable
	UpdateRouteItem = updateRouteItem
	CheckExpired    = checkExpired
	NewMetrics      = newMetrics
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
