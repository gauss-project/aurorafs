package routetab

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"

	"time"
)

type RouteTab interface {

}

///									                  |  -- (nextHop)
///									   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
///      neighbor <--|
///					 |				                  |  -- (nextHop)
///					 |				   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
type RouteItem  struct{
	ttl 		uint8
	neighbor    boson.Address
	nextHop 	[]RouteItem
}

type RouteTable struct {
	items map[common.Hash]RouteItem
}



type pendCallResItem struct {
	resCh   chan RouteItem
	expired time.Time
}
// pendFindRouteReqItem is used to store find_route request from libp2p
type pendFindRouteReqItem struct {
	resCh   chan RouteItem
	expired time.Time
}

type pendingCallResArray []pendCallResItem
type pendCallResTab map[common.Hash]pendingCallResArray


type route_service struct {
	pendingCallRes pendCallResTab
}
func NewRouteTab() RouteTable{
	//load route table from db only those valid item will be loaded

	//start route service


	return RouteTable{}
}

//implement for Closer Interface
func (rt *RouteTable) Close() error {
	//backup data to db
	return nil
}