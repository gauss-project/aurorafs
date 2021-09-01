package routetab

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

const (
	protocolName      = "router"
	protocolVersion   = "1.0.0"
	streamOnRouteReq  = "onRouteReq"
	streamOnRouteResp = "onRouteResp"
)

type RouteTab interface {
	GetRoute(ctx context.Context, dest boson.Address) (routes []RouteItem, err error)
	FindRoute(ctx context.Context, dest boson.Address) (route []RouteItem, err error)
}

type Service struct {
	addr         boson.Address
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	pendingCalls *pendCallResTab
	routeTable   *routeTable
	topology     topology.Driver
}

func New(addr boson.Address, ctx context.Context, streamer p2p.Streamer, topology topology.Driver, store storage.StateStorer, logger logging.Logger) *Service {
	// load route table from db only those valid item will be loaded

	met := newMetrics()

	service := &Service{
		addr:         addr,
		streamer:     streamer,
		logger:       logger,
		topology:     topology,
		pendingCalls: newPendCallResTab(addr, logger, met),
		routeTable:   newRouteTable(store, logger, met),
		metrics:      met,
	}
	// start route service
	go func() {
		service.start(ctx)
	}()
	return service
}

// Close implement for Closer Interface
func (s *Service) Close() error {
	// backup data to db
	return nil
}

func (s *Service) start(ctx context.Context) {
	go func() {
		s.routeTable.Gc(gcTime)
		ticker := time.NewTicker(gcInterval)
		for {
			select {
			case <-ticker.C:
				s.routeTable.Gc(gcTime)
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(pendingInterval)
		for {
			select {
			case <-ticker.C:
				s.pendingCalls.Gc(PendingTimeout)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamOnRouteReq,
				Handler: s.onRouteReq,
			},
			{
				Name:    streamOnRouteResp,
				Handler: s.onRouteResp,
			},
		},
	}
}

func (s *Service) onRouteReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer func() {
		go func() {
			err := stream.FullClose()
			if err != nil {
				s.logger.Warningf("route: sendDataToNode stream.FullClose, %s", err.Error())
			}
		}()
	}()

	var req pb.FindRouteReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteReq read msg: %s", err.Error())
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("route: handlerFindRouteReq received: dest= %s", boson.NewAddress(req.Dest).String())

	s.metrics.FindRouteReqReceivedCount.Inc()
	// passive route save
	go func(path [][]byte) {
		for i, target := range path {
			now := pathToRouteItem(path[i:])
			if len(now) > 0 {
				_ = s.routeTable.Set(boson.NewAddress(target), now)
			}
		}
	}(req.Path)

	dest := boson.NewAddress(req.Dest)

	if len(req.Path) <= int(MaxTTL) {
		// need resp
		routes, err := s.GetRoute(ctx, dest)
		if err != nil {
			s.logger.Debugf("route: handlerFindRouteReq dest= %s route not found", dest.String())
			if !s.isNeighbor(dest) {
				// forward
				forward := s.getNeighbor(dest, req.Alpha)
				for _, v := range forward {
					if !inPath(v.Bytes(), req.Path) {
						// forward
						req.Path = append(req.Path, p.Address.Bytes())
						s.doReq(ctx, s.addr, p2p.Peer{Address: v}, dest, &req, nil)
						s.logger.Tracef("route: handlerFindRouteReq dest= %s forward ro %s", dest.String(), v.String())
					}
					// discard
					s.logger.Tracef("route: handlerFindRouteReq dest= %s discard", dest.String())
				}
			} else {
				// dest in neighbor then resp
				s.doResp(ctx, p, dest, routes)
				s.logger.Tracef("route: handlerFindRouteReq dest= %s in neighbor", dest.String())
			}
		} else {
			// have route resp
			s.doResp(ctx, p, dest, routes)
			s.logger.Tracef("route: handlerFindRouteReq dest= %s in route table", dest.String())
		}
	}
	return nil
}

func (s *Service) onRouteResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer func() {
		go func() {
			err := stream.FullClose()
			if err != nil {
				s.logger.Warningf("route: sendDataToNode stream.FullClose, %s", err.Error())
			}
		}()
	}()

	resp := pb.FindRouteResp{}
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteResp read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("route: handlerFindRouteResp received: dest= %s", boson.NewAddress(resp.Dest).String())

	s.metrics.FindRouteRespReceivedCount.Inc()

	go s.saveRespRouteItem(ctx, p.Address, resp)
	return nil
}

func (s *Service) FindRoute(ctx context.Context, dest boson.Address) (routes []RouteItem, err error) {
	routes, err = s.GetRoute(ctx, dest)
	if err == ErrNotFound {
		s.logger.Debugf("route: FindRoute dest %s", dest.String(), err)
		if s.isNeighbor(dest) {
			err = fmt.Errorf("route: FindRoute dest %s is neighbor", dest.String())
			return
		}
		forward := s.getNeighbor(dest, defaultNeighborAlpha)
		if len(forward) > 0 {
			ct, cancel := context.WithTimeout(ctx, PendingTimeout)
			defer cancel()
			resCh := make(chan struct{}, len(forward))
			for _, v := range forward {
				req := &pb.FindRouteReq{
					Dest: dest.Bytes(),
					Path: nil,
				}
				s.doReq(ct, s.addr, p2p.Peer{Address: v}, dest, req, resCh)
			}
			select {
			case <-ct.Done():
				close(resCh)
				s.metrics.TotalErrors.Inc()
				err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", dest.String(), PendingTimeout.Seconds())
				s.logger.Errorf(err.Error())
			case <-resCh:
				routes, err = s.GetRoute(ctx, dest)
			}
			return
		}
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: FindRoute dest %s , neighbor len equal 0", dest.String())
		err = fmt.Errorf("neighbor len equal 0")
	}
	return
}

func (s *Service) GetRoute(_ context.Context, dest boson.Address) (routes []RouteItem, err error) {
	return s.routeTable.Get(dest)
}

func (s *Service) saveRespRouteItem(ctx context.Context, neighbor boson.Address, resp pb.FindRouteResp) {
	minTTL := MaxTTL
	if len(resp.RouteItems) == 0 {
		minTTL = 0
	}
	for _, v := range resp.RouteItems {
		if uint8(v.Ttl) < minTTL {
			minTTL = uint8(v.Ttl)
		}
	}
	now := []RouteItem{{
		CreateTime: time.Now().Unix(),
		TTL:        minTTL + 1,
		Neighbor:   neighbor,
		NextHop:    convPbToRouteList(resp.RouteItems),
	}}

	target := boson.NewAddress(resp.Dest)

	_ = s.routeTable.Set(target, now)

	// doing resp
	err := s.pendingCalls.Forward(ctx, s, target, now)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: pendingCalls.Forward %s", err.Error())
		return
	}
}

func (s *Service) getNeighbor(target boson.Address, alpha int32) (forward []boson.Address) {
	forward = make([]boson.Address, 0)
	for i := int32(0); i < alpha; i++ {
		addr, err := s.topology.ClosestPeer(target, false, forward...)
		if err != nil {
			if errors.Is(err, topology.ErrNotFound) {
				break
			}
			s.metrics.TotalErrors.Inc()
			s.logger.Errorf("route: get neighbor: %s", err.Error())
			continue
		}
		forward = append(forward, addr)
	}
	return
}

func (s *Service) isNeighbor(dest boson.Address) (has bool) {
	err := s.topology.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if dest.Equal(address) {
			has = true
			return
		}
		return false, true, nil
	})
	if err != nil {
		s.logger.Warningf("route: isNeighbor %s", err.Error())
	}
	return
}

func (s *Service) doReq(ctx context.Context, src boson.Address, peer p2p.Peer, target boson.Address, req *pb.FindRouteReq, ch chan struct{}) {
	if req.Alpha == 0 {
		req.Alpha = defaultNeighborAlpha
	}
	err := s.pendingCalls.Add(target, src, ch)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doReq pendingCalls.Add: %s", err.Error())
		return
	}
	s.sendDataToNode(ctx, peer, streamOnRouteReq, req)
	s.metrics.FindRouteReqSentCount.Inc()
}

func (s *Service) doResp(ctx context.Context, peer p2p.Peer, dest boson.Address, routes []RouteItem) {
	resp := &pb.FindRouteResp{
		Dest: dest.Bytes(),
		RouteItems: func() []*pb.RouteItem {
			if len(routes) > 0 {
				return convRouteToPbRouteList(routes)
			}
			return []*pb.RouteItem{}
		}(),
	}
	s.sendDataToNode(ctx, peer, streamOnRouteResp, resp)
	s.metrics.FindRouteRespSentCount.Inc()
}

func (s *Service) sendDataToNode(ctx context.Context, peer p2p.Peer, streamName string, msg protobuf.Message) {
	s.logger.Tracef("route: sendDataToNode dest %s ,handler %s", peer.Address.String(), streamName)
	stream, err1 := s.streamer.NewStream(ctx, peer.Address, nil, protocolName, protocolVersion, streamName)
	if err1 != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: sendDataToNode NewStream, err1=%s", err1)
		return
	}
	defer func() {
		go func() {
			err := stream.FullClose()
			if err != nil {
				s.logger.Warningf("route: sendDataToNode stream.FullClose, %s", err.Error())
			}
		}()
	}()
	w := protobuf.NewWriter(stream)
	err := w.WriteMsgWithContext(ctx, msg)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: sendDataToNode write msg, err=%s", err)
	}
	return
}
