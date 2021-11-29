package routetab

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"math/rand"
	"resenje.org/singleflight"
	"sync"
	"time"
)

const (
	ProtocolName         = "router"
	ProtocolVersion      = "2.0.0"
	streamOnRouteReq     = "onRouteReq"
	streamOnRouteResp    = "onRouteResp"
	streamOnFindUnderlay = "onFindUnderlay"
	StreamOnRelay        = "relay"

	peerConnectionAttemptTimeout = 5 * time.Second // Timeout for establishing a new connection with peer.
)

var (
	errOverlayMismatch = errors.New("overlay mismatch")
	errPruneEntry      = errors.New("prune entry")

	MaxTTL               uint8 = 10
	DefaultNeighborAlpha int32 = 2
	gcTime                     = time.Minute * 10
	gcInterval                 = time.Minute
	findTimeOut                = time.Second * 2 // find route timeout
)

type Service struct {
	self         boson.Address
	p2ps         p2p.Service
	logger       logging.Logger
	metrics      metrics
	pendingCalls *pendCallResTab
	routeTable   *Table
	kad          *kademlia.Kad
	config       Config
	singleflight singleflight.Group
}

type Config struct {
	AddressBook addressbook.Interface
	NetworkID   uint64
	LightNodes  *lightnode.Container
	Stream      p2p.Streamer
}

func New(self boson.Address, ctx context.Context, p2ps p2p.Service, kad *kademlia.Kad, store storage.StateStorer, logger logging.Logger) *Service {
	// load route table from db only those valid item will be loaded

	met := newMetrics()

	service := &Service{
		self:         self,
		p2ps:         p2ps,
		logger:       logger,
		kad:          kad,
		pendingCalls: newPendCallResTab(),
		routeTable:   newRouteTable(self, store),
		metrics:      met,
	}
	// start route service
	service.start(ctx)
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
	ti := time.Now()
	s.logger.Infof("loading routes...")
	s.routeTable.ResumeRoutes()
	s.routeTable.ResumePaths()
	s.routeTable.Gc(gcTime)
	s.logger.Infof("loading routes completed, expend %s", time.Since(ti).String())

	go func() {
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
				wg := sync.WaitGroup{}
				wg.Add(2)
				go func() {
					defer wg.Done()
					s.pendingCalls.GcReqLog(PendingTimeout)
				}()
				go func() {
					defer wg.Done()
					s.pendingCalls.GcResItems(PendingTimeout)
				}()
				wg.Wait()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    ProtocolName,
		Version: ProtocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamOnRouteReq,
				Handler: s.onRouteReq,
			},
			{
				Name:    streamOnRouteResp,
				Handler: s.onRouteResp,
			},
			{
				Name:    streamOnFindUnderlay,
				Handler: s.onFindUnderlay,
			},
			{
				Name:    StreamOnRelay,
				Handler: s.onRelay,
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

	var req pb.RouteReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteReq read msg: %s", err.Error())
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(req.Dest)

	s.logger.Tracef("route:%s handlerFindRouteReq received: target=%s", s.self.String(), target.String())

	s.metrics.FindRouteReqReceivedCount.Inc()
	// passive route save
	s.routeTable.SavePaths(req.Paths)

	for _, v := range req.Paths {
		items := v.Items // request path only one
		if len(items) > int(MaxTTL) {
			// discard
			s.logger.Tracef("route:%s handlerFindRouteReq target=%s discard, ttl=%d", s.self.String(), target.String(), len(items))
			return nil
		}
		if inPath(s.self.Bytes(), items) {
			// discard
			s.logger.Tracef("route:%s handlerFindRouteReq target=%s discard, received path contains self.", s.self.String(), target.String())
			return nil
		}
	}

	if s.self.Equal(target) {
		// resp
		s.doRouteResp(ctx, p.Address, target, nil)
		return nil
	}
	if s.IsNeighbor(target) {
		// dest in neighbor
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s in neighbor", s.self.String(), target.String())
		s.doRouteReq(ctx, []boson.Address{target}, p.Address, target, &req, nil)
		return nil
	}

	paths, err := s.GetRoute(ctx, target)
	if err == nil {
		// have route resp
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s in route table", s.self.String(), target.String())
		s.doRouteResp(ctx, p.Address, target, &pb.RouteResp{
			Dest:  target.Bytes(),
			Paths: s.routeTable.convertPathsToPbPaths(paths),
		})
		return nil
	}

	// forward
	skip := make([]boson.Address, 0)
	for _, v := range req.Paths {
		for _, addr := range v.Items {
			skip = append(skip, boson.NewAddress(addr))

		}
	}
	forward := s.getNeighbor(target, req.Alpha, skip...)
	s.doRouteReq(ctx, forward, p.Address, target, &req, nil)
	return nil
}

func (s *Service) onRouteResp(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer func() {
		go func() {
			err := stream.FullClose()
			if err != nil {
				s.logger.Warningf("route: sendDataToNode stream.FullClose, %s", err.Error())
			}
		}()
	}()

	resp := pb.RouteResp{}
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteResp read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(resp.Dest)
	s.logger.Tracef("route:%s handlerFindRouteResp received: dest= %s", s.self.String(), target.String())

	s.metrics.FindRouteRespReceivedCount.Inc()

	s.routeTable.SavePaths(resp.Paths)

	// doing forward resp
	s.respForward(ctx, target, peer.Address, &resp)
	return nil
}

func (s *Service) respForward(ctx context.Context, target, next boson.Address, resp *pb.RouteResp) {
	res := s.pendingCalls.Get(target, next)
	skip := make([]boson.Address, 0)
	for _, v := range res {
		if !v.Src.Equal(s.self) {
			if !v.Src.MemberOf(skip) {
				// forward
				s.doRouteResp(ctx, v.Src, target, resp)
				skip = append(skip, v.Src)
			}
		} else if v.ResCh != nil {
			// sync return
			v.ResCh <- struct{}{}
		}
	}
}

func (s *Service) doRouteReq(ctx context.Context, next []boson.Address, src, target boson.Address, req *pb.RouteReq, ch chan struct{}) {
	if req != nil {
		// forward add sign
		req.Paths = s.routeTable.generatePaths(req.Paths)
	} else {
		req = &pb.RouteReq{
			Dest:  target.Bytes(),
			Alpha: DefaultNeighborAlpha,
			Paths: s.routeTable.generatePaths(nil),
		}
	}
	for _, v := range next {
		has := s.pendingCalls.Add(boson.NewAddress(req.Dest), src, v, ch)
		if !has {
			s.sendDataToNode(ctx, v, streamOnRouteReq, req)
			s.metrics.FindRouteReqSentCount.Inc()
		}
	}
}

func (s *Service) doRouteResp(ctx context.Context, src, target boson.Address, resp *pb.RouteResp) {
	if resp != nil {
		resp.Paths = s.routeTable.generatePaths(resp.Paths)
	} else {
		resp = &pb.RouteResp{
			Dest:  target.Bytes(),
			Paths: s.routeTable.generatePaths(nil),
		}
	}
	s.sendDataToNode(ctx, src, streamOnRouteResp, resp)
	s.metrics.FindRouteRespSentCount.Inc()
}

func (s *Service) sendDataToNode(ctx context.Context, peer boson.Address, streamName string, msg protobuf.Message) {
	s.logger.Tracef("route:%s sendDataToNode to %s %s", s.self.String(), peer.String(), streamName)
	stream, err1 := s.config.Stream.NewStream(ctx, peer, nil, ProtocolName, ProtocolVersion, streamName)
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

func (s *Service) getNeighbor(target boson.Address, alpha int32, skip ...boson.Address) (forward []boson.Address) {
	if alpha <= 0 {
		alpha = DefaultNeighborAlpha
	}
	depth := s.kad.NeighborhoodDepth()
	po := boson.Proximity(s.self.Bytes(), target.Bytes())

	var now []boson.Address
	if po < depth {
		list := s.kad.ConnectedPeers().BinPeers(po)
		now = skipPeers(list, skip)
	} else {
		_ = s.kad.EachNeighbor(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				now = append(now, address)
			}
			return false, false, nil
		})
	}
	if len(now) == 0 {
		list := s.kad.ConnectedPeers().BinPeers(depth - 1)
		now = skipPeers(list, skip)
	}
	forward, _ = s.kad.RandomSubset(now, int(alpha))
	return
}

func (s *Service) IsNeighbor(dest boson.Address) (has bool) {
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

func (s *Service) GetRoute(_ context.Context, dest boson.Address) ([]*Path, error) {
	return s.routeTable.Get(dest)
}

func (s *Service) FindRoute(ctx context.Context, target boson.Address, timeout ...time.Duration) (paths []*Path, err error) {
	if s.self.Equal(target) {
		err = fmt.Errorf("target=%s is self", target.String())
		return
	}
	forward := s.getNeighbor(target, DefaultNeighborAlpha, target)
	if len(forward) > 0 {
		if len(timeout) > 0 {
			findTimeOut = timeout[0]
		}
		ct, cancel := context.WithTimeout(ctx, findTimeOut)
		defer cancel()
		resCh := make(chan struct{}, len(forward))
		s.doRouteReq(ct, forward, s.self, target, nil, resCh)
		remove := func() {
			for _, v := range forward {
				s.pendingCalls.Delete(target, v)
			}
		}
		select {
		case <-ct.Done():
			remove()
			err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", target.String(), findTimeOut.Seconds())
			s.logger.Debugf(err.Error())
		case <-ctx.Done():
			remove()
			err = fmt.Errorf("route: FindRoute dest %s praent ctx.Done %s", target.String(), ctx.Err())
			s.logger.Debugf(err.Error())
		case <-resCh:
			paths, err = s.GetRoute(ctx, target)
		}
		return
	}
	s.metrics.TotalErrors.Inc()
	s.logger.Errorf("route: FindRoute target=%s , neighbor notfound", target.String())
	err = fmt.Errorf("neighbor notfound")
	return
}

func (s *Service) DelRoute(ctx context.Context, target boson.Address) error {
	route, err := s.routeTable.Get(target)
	if err != nil {
		return err
	}
	for _, v := range route {
		s.routeTable.Delete(v)
	}
	return nil
}

func (s *Service) GetTargetNeighbor(ctx context.Context, target boson.Address, limit int) (addresses []boson.Address, err error) {
	var list interface{}
	key := "GetTargetNeighbor_" + target.String()
	list, _, err = s.singleflight.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		var routes []*Path
		routes, err = s.getOrFindRoute(ctx, target)
		if err != nil {
			return nil, err
		}
		addresses = s.getClosestNeighborLimit(target, routes, limit)
		if len(addresses) == 0 {
			routes, err = s.FindRoute(context.TODO(), target)
			if err != nil {
				return nil, err
			}
			addresses = s.getClosestNeighborLimit(target, routes, limit)
			if len(addresses) == 0 {
				return nil, errors.New("neighbor not found")
			}
		}
		return addresses, nil
	})
	if err != nil {
		return nil, err
	}
	if list != nil {
		addresses = list.([]boson.Address)
	}
	for _, v := range addresses {
		s.logger.Debugf("get dest=%s neighbor %v", target, v.String())
	}
	return
}

func (s *Service) getClosestNeighborLimit(target boson.Address, routes []*Path, limit int) (out []boson.Address) {
	has := make(map[string]bool)
	for _, path := range routes {
		length := len(path.Items)
		if !s.IsNeighbor(path.Items[length-1]) {
			continue
		}
		for k, v := range path.Items {
			if v.Equal(target) {
				if k-1 >= 0 {
					has[path.Items[k-1].String()] = true
				}
				if k+1 < length {
					has[path.Items[k+1].String()] = true
				}
				break
			}
		}
		if len(has) >= limit {
			break
		}
	}
	for hex := range has {
		out = append(out, boson.MustParseHexAddress(hex))
	}
	return
}

func (s *Service) Connect(ctx context.Context, target boson.Address) error {
	if target.Equal(s.self) {
		return errors.New("cannot connected to self")
	}
	key := "route_connect_" + target.String()
	_, _, err := s.singleflight.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		if !s.isConnected(ctx, target) {
			err := s.connect(ctx, target)
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *Service) isConnected(ctx context.Context, target boson.Address) bool {
	var isConnected bool
	findFun := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if target.Equal(address) {
			isConnected = true
			return true, false, nil
		}
		return false, false, nil
	}
	_ = s.kad.EachPeer(findFun)
	if isConnected {
		s.logger.Debugf("route: connect target in neighbor")
		return true
	}
	_ = s.config.LightNodes.EachPeer(findFun)
	if isConnected {
		s.logger.Debugf("route: connect target(light) in neighbor")
		return true
	}
	return false
}

func (s *Service) connect(ctx context.Context, peer boson.Address) (err error) {
	needFindUnderlay := false
	auroraAddr, err := s.config.AddressBook.Get(peer)
	switch {
	case errors.Is(err, addressbook.ErrNotFound):
		s.logger.Debugf("route: empty address book entry for peer %s", peer)
		s.kad.KnownPeers().Remove(peer)
		needFindUnderlay = true
	case err != nil:
		s.logger.Debugf("route: failed to get address book entry for peer %s: %v", peer, err)
		needFindUnderlay = true
	default:
	}

	if needFindUnderlay {
		auroraAddr, err = s.FindUnderlay(ctx, peer)
		if err != nil {
			return err
		}
	}

	remove := func(peer boson.Address) {
		s.kad.KnownPeers().Remove(peer)
		if err := s.config.AddressBook.Remove(peer); err != nil {
			s.logger.Debugf("route: could not remove peer %s from addressBook", peer)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, peerConnectionAttemptTimeout)
	defer cancel()

	s.logger.Tracef("route: connect to overlay=%s,underlay=%s", peer, auroraAddr.Underlay.String())

	i, err := s.p2ps.Connect(ctx, auroraAddr.Underlay)
	switch {
	case errors.Is(err, p2p.ErrDialLightNode):
		return errPruneEntry
	case errors.Is(err, p2p.ErrAlreadyConnected):
		if !i.Address.Equal(peer) {
			return errOverlayMismatch
		}
		return nil
	case errors.Is(err, context.Canceled):
		return err
	case err != nil:
		s.logger.Debugf("route: could not connect to peer %s: %v", peer, err)
		s.metrics.TotalOutboundConnectionFailedAttempts.Inc()
		remove(peer)
		return err
	case !i.Address.Equal(peer):
		_ = s.p2ps.Disconnect(peer, errOverlayMismatch.Error())
		_ = s.p2ps.Disconnect(i.Address, errOverlayMismatch.Error())
		return errOverlayMismatch
	}

	s.metrics.TotalOutboundConnections.Inc()
	s.kad.Outbound(*i)

	return nil
}

func (s *Service) onFindUnderlay(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		go func() {
			if err != nil {
				_ = stream.Reset()
			} else {
				go stream.FullClose()
			}
		}()
	}()

	req := pb.UnderlayReq{}
	if err = r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: onFindUnderlay read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(req.Dest)
	s.logger.Tracef("find underlay dest %s receive: from %s", target.String(), p.Address.String())

	address, err := s.config.AddressBook.Get(target)
	if err == nil {
		err = w.WriteMsgWithContext(ctx, &pb.UnderlayResp{
			Dest:      req.Dest,
			Underlay:  address.Underlay.Bytes(),
			Signature: address.Signature,
		})
		if err != nil {
			return err
		}
		s.logger.Tracef("find underlay dest %s send: to %s", target.String(), p.Address.String())
		return nil
	}
	// get route
	next, err := s.GetNextHopRandomOrFind(ctx, target)
	if err != nil {
		return err
	}
	stream2, err := s.config.Stream.NewStream(ctx, next, nil, ProtocolName, ProtocolVersion, streamOnFindUnderlay)
	if err != nil {
		return err
	}
	defer func() {
		go func() {
			if err != nil {
				_ = stream2.Reset()
			} else {
				go stream2.FullClose()
			}
		}()
	}()
	w2, r2 := protobuf.NewWriterAndReader(stream2)
	err = w2.WriteMsgWithContext(ctx, &req)
	if err != nil {
		return err
	}
	s.logger.Tracef("find underlay dest %s forward: to %s", target, next)
	resp := &pb.UnderlayResp{}
	if err = r2.ReadMsgWithContext(ctx, resp); err != nil {
		content := fmt.Sprintf("route: onFindUnderlay read resp msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("find underlay dest %s receive2: from %s", target, next)

	address, err = aurora.ParseAddress(resp.Underlay, resp.Dest, resp.Signature, s.config.NetworkID)
	if err != nil {
		s.logger.Errorf("find underlay dest %s parse err %s", target.String(), err.Error())
		return err
	}
	err = w.WriteMsgWithContext(ctx, resp)
	if err != nil {
		return err
	}
	s.logger.Tracef("find underlay dest %s send2: to %s", target.String(), p.Address.String())
	err = s.config.AddressBook.Put(address.Overlay, *address)
	return err
}

func (s *Service) getOrFindRoute(ctx context.Context, target boson.Address) (paths []*Path, err error) {
	paths, err = s.GetRoute(ctx, target)
	if errors.Is(err, ErrNotFound) {
		paths, err = s.FindRoute(ctx, target)
	}
	return
}

func (s *Service) FindUnderlay(ctx context.Context, target boson.Address) (addr *aurora.Address, err error) {
	next, err := s.GetNextHopRandomOrFind(ctx, target)
	if err != nil {
		return nil, err
	}
	stream, err := s.config.Stream.NewStream(ctx, next, nil, ProtocolName, ProtocolVersion, streamOnFindUnderlay)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)
	req := pb.UnderlayReq{
		Dest: target.Bytes(),
	}
	if err = w.WriteMsgWithContext(ctx, &req); err != nil {
		s.logger.Errorf("find underlay dest %s req err %s", target.String(), err.Error())
		return nil, err
	}

	s.routeTable.updateUsedTime(target, next)

	s.logger.Tracef("find underlay dest %s send to %s", target, next)

	resp := &pb.UnderlayResp{}
	if err = r.ReadMsgWithContext(ctx, resp); err != nil {
		s.logger.Errorf("find underlay dest %s read msg: %s", target, err.Error())
		return nil, err
	}
	s.logger.Tracef("find underlay dest %s receive: from %s", target, next)

	addr, err = aurora.ParseAddress(resp.Underlay, resp.Dest, resp.Signature, s.config.NetworkID)
	if err != nil {
		s.logger.Errorf("find underlay dest %s parse err %s", target.String(), err.Error())
		return nil, err
	}
	err = s.config.AddressBook.Put(addr.Overlay, *addr)
	if err != nil {
		return nil, err
	}
	return addr, nil
}

func (s *Service) GetNextHopRandomOrFind(ctx context.Context, target boson.Address) (next boson.Address, err error) {
	next = s.getNextHopRandom(target)
	if next.IsZero() {
		_, err = s.FindRoute(ctx, target)
		if err != nil {
			return
		}
		next = s.getNextHopRandom(target)
		if next.IsZero() {
			err = fmt.Errorf("nexthop not found")
			return
		}
	}
	return
}

func (s *Service) getNextHopRandom(target boson.Address) (next boson.Address) {
	list := s.getNextHopEffective(target)
	if len(list) > 0 {
		k := rand.Intn(len(list))
		s.routeTable.updateUsedTime(target, list[k])
		return list[k]
	}
	return boson.ZeroAddress
}

func (s *Service) getNextHopEffective(target boson.Address) (next []boson.Address) {
	list := s.routeTable.GetNextHop(target)
	for _, v := range list {
		if s.IsNeighbor(v) {
			next = append(next, v)
		}
	}
	return next
}

func (s *Service) onRelay(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		go func() {
			if err != nil {
				_ = stream.Reset()
			} else {
				_ = stream.FullClose()
			}
		}()
	}()

	var (
		target       boson.Address
		next         boson.Address
		forwardStram p2p.Stream
		w1           protobuf.Writer
		r1           protobuf.Reader
		called       bool // the called handler
	)
	reallyDataChan := make(chan []byte, 1)
	errChan := make(chan error, 1)
	readReqChan := make(chan *pb.RouteRelayReq, 1)
	readRespChan := make(chan *pb.RouteRelayResp, 1)

	defer func() {
		if forwardStram != nil {
			go func() {
				if err != nil {
					_ = forwardStram.Reset()
				} else {
					_ = forwardStram.FullClose()
				}
			}()
		}
	}()

	// read request
	go func() {
		for {
			select {
			case <-errChan:
				s.logger.Tracef("route: onRelay read request done.")
				return
			default:
				req := &pb.RouteRelayReq{}
				if err = r.ReadMsgWithContext(ctx, req); err != nil {
					content := fmt.Sprintf("route: onRelay read req msg from src: %s", err.Error())
					s.logger.Errorf(content)
					errChan <- fmt.Errorf(content)
					break
				}
				readReqChan <- req
			}
		}
	}()

	// read response
	readResp := func() {
		for {
			select {
			case <-errChan:
				s.logger.Tracef("route: onRelay read response done.")
				return
			default:
				resp := &pb.RouteRelayResp{}
				if err = r1.ReadMsgWithContext(ctx, resp); err != nil {
					content := fmt.Sprintf("route: onRelay read resp msg from src: %s", err.Error())
					s.logger.Errorf(content)
					errChan <- fmt.Errorf(content)
					break
				}
				readRespChan <- resp
				s.logger.Tracef("route: onRelay target %s receive: from %s", target, next)
			}
		}
	}

	for {
		select {
		case req := <-readReqChan:
			target = boson.NewAddress(req.Dest)
			s.logger.Tracef("route: onRelay target %s receive: from %s", target.String(), p.Address.String())
			if target.Equal(s.self) {
				reallyDataChan <- req.Data
				if !called {
					called = true
					go func() {
						s.logger.Debugf("route: call handel %s/%s/%s", string(req.ProtocolName), string(req.ProtocolVersion), string(req.StreamName))
						errChan <- s.p2ps.CallHandler(ctx, req, reallyDataChan, p, stream)
					}()
				}
				break
			}
			if forwardStram == nil {
				// jump next
				if s.IsNeighbor(target) {
					next = target
				} else {
					next, err = s.GetNextHopRandomOrFind(ctx, target)
					if err != nil {
						s.logger.Debugf("route: onRelay target %s nextHop not found", target)
						errChan <- err
						break
					}
				}
				forwardStram, err = s.config.Stream.NewStream(ctx, next, stream.Headers(), ProtocolName, ProtocolVersion, StreamOnRelay)
				if err != nil {
					errChan <- err
					break
				}
				w1, r1 = protobuf.NewWriterAndReader(forwardStram)
				go readResp()
				s.logger.Tracef("route: relay stream created, target %s next %s", target, next)
			}
			if err = w1.WriteMsgWithContext(ctx, req); err != nil {
				content := fmt.Sprintf("route: onRelay forward msg: %s", err.Error())
				s.logger.Errorf(content)
				errChan <- fmt.Errorf(content)
				break
			}
			s.logger.Tracef("route: onRelay forward target %s to %s", target, next)
		case resp := <-readRespChan:
			if err = w.WriteMsgWithContext(ctx, resp); err != nil {
				content := fmt.Sprintf("route: onRelay write msg to src: %s", err.Error())
				s.logger.Errorf(content)
				errChan <- fmt.Errorf(content)
				break
			}
		case err = <-errChan:
			return err
		}
	}
}
