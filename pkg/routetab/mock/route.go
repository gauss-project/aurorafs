package mock

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab"
)

type mockRouteTable struct {
}

func NewMockRouteTable() mockRouteTable {
	return mockRouteTable{}
}

func (r *mockRouteTable) GetRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, routes []routetab.RouteItem, err error) {
	return nil, []routetab.RouteItem{}, nil
}
func (r *mockRouteTable) FindRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, route []routetab.RouteItem, err error) {
	return nil, []routetab.RouteItem{}, nil
}
func (r *mockRouteTable) Connect(ctx context.Context, target boson.Address) error {
	return nil
}

func (r *mockRouteTable) GetTargetNeighbor(ctx context.Context, target boson.Address, limit int) (addresses []boson.Address, err error) {
	return nil, nil
}
func (r *mockRouteTable) IsNeighbor(dest boson.Address) (has bool) {
	return false
}
