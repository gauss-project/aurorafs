package routetab

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"

	"time"
)
const (
	protocolName    = "router"
	protocolVersion = "1.0.0"
	streamName      = "router"
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



type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection
	FindRouteReqSentCount     	prometheus.Counter
	FindRouteRespSentCount     	prometheus.Counter
	FindRouteReqReceivedCount 	prometheus.Counter
	FindRouteRespReceivedCount 	prometheus.Counter
}
type Service struct {
	streamer p2p.Streamer
	logger   logging.Logger
	tracer   *tracing.Tracer
	metrics  metrics
	pendingCalls pendCallResTab
	routeTable RouteTable
}
func NewRouteTab(ctx context.Context,streamer p2p.Streamer, logger logging.Logger, tracer *tracing.Tracer) Service{
	//load route table from db only those valid item will be loaded

	service := Service{

	}
	//start route service

	go func() {
		service.start(ctx)
	}()
	return service
}

//implement for Closer Interface
func (rt *Service) Close() error {
	//backup data to db
	return nil
}

func (svc *Service) start(ctx context.Context){

}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
	}
}

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {

	return nil
}