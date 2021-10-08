package mock

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab"
)

type MockRouteTable struct {
	RejectAddrList []boson.Address
	NeighborMap    map[string][]boson.Address
}

func NewMockRouteTable() MockRouteTable {
	return MockRouteTable{}
}

func (r *MockRouteTable) GetRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, routes []routetab.RouteItem, err error) {
	return nil, []routetab.RouteItem{}, nil
}
func (r *MockRouteTable) FindRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, route []routetab.RouteItem, err error) {
	return nil, []routetab.RouteItem{}, nil
}
func (r *MockRouteTable) Connect(ctx context.Context, target boson.Address) error {
	for _, node := range r.RejectAddrList {
		if target.Equal(node) {
			return fmt.Errorf("reject")
		}
	}
	return nil
}

func (r *MockRouteTable) GetTargetNeighbor(ctx context.Context, target boson.Address, limit int) (addresses []boson.Address, err error) {
	return r.NeighborMap[target.String()], nil
}
func (r *MockRouteTable) IsNeighbor(dest boson.Address) (has bool) {
	return false
}
