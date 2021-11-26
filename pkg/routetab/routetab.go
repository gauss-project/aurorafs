package routetab

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"time"
)

type RouteTab interface {
	GetRoute(ctx context.Context, dest boson.Address) (paths []*Path, err error)
	FindRoute(ctx context.Context, dest boson.Address, timeout ...time.Duration) (paths []*Path, err error)
	DelRoute(ctx context.Context, dest boson.Address) (err error)
	Connect(ctx context.Context, dest boson.Address) error
	GetTargetNeighbor(ctx context.Context, dest boson.Address, limit int) (addresses []boson.Address, err error)
	IsNeighbor(dest boson.Address) (has bool)
	FindUnderlay(ctx context.Context, target boson.Address) (addr *aurora.Address, err error)
}

type GetNextHop interface {
	GetNextHopRandomOrFind(ctx context.Context, target boson.Address) (next boson.Address, err error)
}
