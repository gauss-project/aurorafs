package routetab

import (
	"context"
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
}

type RouteRelayData struct {
	Src             []byte
	Dest            []byte
	ProtocolName    []byte
	ProtocolVersion []byte
	StreamName      []byte
	Data            []byte
}
