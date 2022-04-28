package routetab

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
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

type RelayStream interface {
	GetNextHopRandomOrFind(ctx context.Context, target boson.Address, skips ...boson.Address) (next boson.Address, err error)
	PackRelayReq(ctx context.Context, stream p2p.VirtualStream, req *pb.RouteRelayReq)
	PackRelayResp(ctx context.Context, stream p2p.VirtualStream, req chan *pb.RouteRelayReq)
}
