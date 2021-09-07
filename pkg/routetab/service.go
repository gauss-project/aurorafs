package routetab

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
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

	peerConnectionAttemptTimeout = 5 * time.Second // Timeout for establishing a new connection with peer.
)

var (
	errOverlayMismatch = errors.New("overlay mismatch")
	errPruneEntry      = errors.New("prune entry")
)

type RouteTab interface {
	GetRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, routes []RouteItem, err error)
	FindRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, route []RouteItem, err error)
	Connect(ctx context.Context, target boson.Address) error
}

type Service struct {
	addr         boson.Address
	p2ps         p2p.StreamerConnect
	logger       logging.Logger
	metrics      metrics
	pendingCalls *pendCallResTab
	routeTable   *routeTable
	kad          *kademlia.Kad
	config       Config
}

type Config struct {
	AddressBook addressbook.Interface
	NetworkID   uint64
}

func New(addr boson.Address, ctx context.Context, p2ps p2p.StreamerConnect, kad *kademlia.Kad, store storage.StateStorer, logger logging.Logger) *Service {
	// load route table from db only those valid item will be loaded

	met := newMetrics()

	service := &Service{
		addr:         addr,
		p2ps:         p2ps,
		logger:       logger,
		kad:          kad,
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

func (s *Service) SetConfig(cfg Config) {
	s.config = cfg
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

	target := boson.NewAddress(req.Dest)

	if len(req.Path) > int(MaxTTL) {
		// discard
		s.logger.Tracef("route: handlerFindRouteReq dest= %s discard ttl= %d", target.String(), len(req.Path))
		return nil
	}
	if inPath(s.addr.Bytes(), req.Path) {
		// discard
		s.logger.Tracef("route: handlerFindRouteReq dest= %s discard received path contains self", target.String())
		return nil
	}
	// need resp
	if s.isNeighbor(target) {
		// dest in neighbor then resp
		dest, _ := s.config.AddressBook.Get(target)
		s.doRouteResp(ctx, p.Address, dest, []RouteItem{})
		s.logger.Tracef("route: handlerFindRouteReq dest= %s in neighbor", target.String())
		return nil
	}
	dest, routes, err := s.GetRoute(ctx, target)
	if err == nil {
		if len(req.Path)+minTTL(routes) > int(MaxTTL) {
			// discard
			s.logger.Tracef("route: handlerFindRouteReq dest= %s discard ttl= %d", target.String(), len(req.Path)+minTTL(routes))
			return nil
		}
		// have route resp
		s.doRouteResp(ctx, p.Address, dest, routes)
		s.logger.Tracef("route: handlerFindRouteReq dest= %s in route table", target.String())
		return nil
	}
	// forward
	forward := s.getNeighbor(target, req.Alpha)
	for _, v := range forward {
		if !inPath(v.Bytes(), req.Path) {
			// forward
			req.Path = append(req.Path, p.Address.Bytes())
			s.doRouteReq(ctx, s.addr, v, target, &req, nil)
			s.logger.Tracef("route: handlerFindRouteReq dest= %s forward ro %s", target.String(), v.String())
			continue
		}
		// discard
		s.logger.Tracef("route: handlerFindRouteReq dest= %s discard forward= %s", target.String(), v.String())
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

func (s *Service) FindRoute(ctx context.Context, target boson.Address) (dest *aurora.Address, routes []RouteItem, err error) {
	dest, routes, err = s.GetRoute(ctx, target)
	if err != nil {
		s.logger.Debugf("route: FindRoute dest %s", target.String(), err)
		if s.isNeighbor(target) {
			err = fmt.Errorf("route: FindRoute dest %s is neighbor", target.String())
			return
		}
		forward := s.getNeighbor(target, defaultNeighborAlpha)
		if len(forward) > 0 {
			ct, cancel := context.WithTimeout(ctx, PendingTimeout)
			defer cancel()
			resCh := make(chan struct{}, len(forward))
			for _, v := range forward {
				req := &pb.FindRouteReq{
					Dest: target.Bytes(),
					Path: nil,
				}
				s.doRouteReq(ct, s.addr, v, target, req, resCh)
			}
			select {
			case <-ct.Done():
				close(resCh)
				s.metrics.TotalErrors.Inc()
				err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", target.String(), PendingTimeout.Seconds())
				s.logger.Errorf(err.Error())
			case <-resCh:
				dest, routes, err = s.GetRoute(ctx, target)
			}
			return
		}
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: FindRoute dest %s , neighbor len equal 0", target.String())
		err = fmt.Errorf("neighbor len equal 0")
	}
	return
}

func (s *Service) GetRoute(_ context.Context, target boson.Address) (dest *aurora.Address, routes []RouteItem, err error) {
	routes, err = s.routeTable.Get(target)
	if err != nil {
		return
	}
	dest, err = s.config.AddressBook.Get(target)
	if err != nil {
		s.routeTable.Remove(target)
	}
	return
}

func (s *Service) Connect(ctx context.Context, target boson.Address) error {
	var isConnected bool
	_ = s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if target.Equal(address) {
			isConnected = true
			return true, false, nil
		}
		return false, false, nil
	})
	if isConnected {
		return nil
	}
	return s.connect(ctx, target)
}

func (s *Service) connect(ctx context.Context, peer boson.Address) (err error) {
	var needFindUnderlay bool
	auroraAddr, err := s.config.AddressBook.Get(peer)
	switch {
	case errors.Is(err, addressbook.ErrNotFound):
		s.logger.Debugf("route: empty address book entry for peer %q", peer)
		s.kad.KnownPeer().Remove(peer)
		needFindUnderlay = true
	case err != nil:
		s.logger.Debugf("route: failed to get address book entry for peer %q: %v", peer, err)
		needFindUnderlay = true
	}
	remove := func(peer boson.Address) {
		s.kad.KnownPeer().Remove(peer)
		if err := s.config.AddressBook.Remove(peer); err != nil {
			s.logger.Debugf("route: could not remove peer %q from addressBook", peer)
		}
	}
	if needFindUnderlay {
		auroraAddr, _, err = s.FindRoute(ctx, peer)
		if err != nil {
			return err
		}
	}
	s.logger.Infof("route: attempting to connect to peer %q", peer)

	ctx, cancel := context.WithTimeout(ctx, peerConnectionAttemptTimeout)
	defer cancel()

	s.metrics.TotalOutboundConnectionAttempts.Inc()

	switch i, err := s.p2ps.Connect(ctx, auroraAddr.Underlay); {
	case errors.Is(err, p2p.ErrDialLightNode):
		return errPruneEntry
	case errors.Is(err, p2p.ErrAlreadyConnected):
		if !i.Overlay.Equal(peer) {
			return errOverlayMismatch
		}
		return nil
	case errors.Is(err, context.Canceled):
		return err
	case err != nil:
		s.logger.Debugf("could not connect to peer %q: %v", peer, err)
		s.metrics.TotalOutboundConnectionFailedAttempts.Inc()
		remove(peer)
		return err
	case !i.Overlay.Equal(peer):
		_ = s.p2ps.Disconnect(peer)
		_ = s.p2ps.Disconnect(i.Overlay)
		return errOverlayMismatch
	}
	return nil
}

func (s *Service) saveRespRouteItem(ctx context.Context, neighbor boson.Address, resp pb.FindRouteResp) {
	address, err := aurora.ParseAddress(resp.Underlay, resp.Dest, resp.Signature, s.config.NetworkID)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: target aurora.ParseAddress %s", err.Error())
		return
	}

	target := boson.NewAddress(resp.Dest)

	err = s.config.AddressBook.Put(target, *address)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: target addressBook.Put %s", err.Error())
		return
	}
	s.kad.AddPeers(target)

	now := []RouteItem{{
		CreateTime: time.Now().Unix(),
		TTL:        minTTLPb(resp.RouteItems) + 1,
		Neighbor:   neighbor,
		NextHop:    convPbToRouteList(resp.RouteItems),
	}}

	_ = s.routeTable.Set(target, now)

	// doing resp
	err = s.pendingCalls.Forward(ctx, s, address, now)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: pendingCalls.Forward %s", err.Error())
		return
	}
}

func (s *Service) getNeighbor(target boson.Address, alpha int32) (forward []boson.Address) {
	forward, _ = s.kad.ClosestPeers(target, int(alpha))
	return
}

func (s *Service) isNeighbor(dest boson.Address) (has bool) {
	err := s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if dest.Equal(address) {
			has = true
			return
		}
		return false, false, nil
	})
	if err != nil {
		s.logger.Warningf("route: isNeighbor %s", err.Error())
	}
	return
}

func (s *Service) doRouteReq(ctx context.Context, src, peer, target boson.Address, req *pb.FindRouteReq, ch chan struct{}) {
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

func (s *Service) doRouteResp(ctx context.Context, peer boson.Address, target *aurora.Address, routes []RouteItem) {
	resp := &pb.FindRouteResp{
		Dest:      target.Overlay.Bytes(),
		Underlay:  target.Underlay.Bytes(),
		Signature: target.Signature,
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

func (s *Service) sendDataToNode(ctx context.Context, peer boson.Address, streamName string, msg protobuf.Message) {
	s.logger.Tracef("route: sendDataToNode dest %s ,handler %s", peer.String(), streamName)
	stream, err1 := s.p2ps.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
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
}
