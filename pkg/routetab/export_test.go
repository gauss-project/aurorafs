// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package routetab

import "github.com/gauss-project/aurorafs/pkg/boson"

var (
	PathToUpdateRouteItem = pathToUpdateRouteItem
	PathToRouteItem = pathToRouteItem
)

func (i *RouteItem) GetCreateTime() int64 {
	return i.createTime
}

func (i *RouteItem) GetTTL() uint8 {
	return i.ttl
}

func (i *RouteItem) GetNeighbor() boson.Address {
	return i.neighbor
}

func (i *RouteItem) GetNextHop() []RouteItem {
	return i.nextHop
}