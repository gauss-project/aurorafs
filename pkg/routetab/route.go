package routetab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"resenje.org/singleflight"
)

const (
	ProtocolName           = "router"
	ProtocolVersion        = "3.0.0"
	StreamOnRelay          = "relay"
	StreamOnRelayConnChain = "relayConnChain"
	streamOnRouteReq       = "onRouteReq"
	streamOnRouteResp      = "onRouteResp"
	streamOnFindUnderlay   = "onFindUnderlay"
)

var (
	MaxTTL        int32 = 10
	NeighborAlpha int32 = 2
	gcTime              = time.Minute * 10
	gcInterval          = time.Minute
	findTimeOut         = time.Second * 3 // find route timeout
)

const (
	uTypeZero int32 = iota // Don't do anything
	uTypeTarget
)

type Service struct {
	self          boson.Address
	p2ps          p2p.Service
	stream        p2p.Streamer
	logger        logging.Logger
	metrics       metrics
	pendingCalls  *pendCallResTab
	routeTable    *Table
	kad           *kademlia.Kad
	lightNodes    *lightnode.Container
	singleflight  singleflight.Group
	networkID     uint64
	addressbook   addressbook.Interface
	findRouteCall map[string][]chan *finRoutePending
	findRouteMix  sync.Mutex
}

type finRoutePending struct {
	err error
	res []*Path
}

type Options struct {
	Alpha int32
}

func New(self boson.Address,
	ctx context.Context,
	p2ps p2p.Service,
	stream p2p.Streamer,
	addressbook addressbook.Interface,
	networkID uint64,
	lightNodes *lightnode.Container,
	kad *kademlia.Kad,
	store storage.StateStorer,
	logger logging.Logger,
	o Options) *Service {
	// load route table from db only those valid item will be loaded

	met := newMetrics()

	service := &Service{
		self:          self,
		p2ps:          p2ps,
		stream:        stream,
		logger:        logger,
		addressbook:   addressbook,
		networkID:     networkID,
		lightNodes:    lightNodes,
		kad:           kad,
		pendingCalls:  newPendCallResTab(),
		routeTable:    newRouteTable(self, store),
		metrics:       met,
		findRouteCall: make(map[string][]chan *finRoutePending),
	}

	if o.Alpha > 0 {
		NeighborAlpha = o.Alpha
	}

	// start route service
	service.start(ctx)
	return service
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
			{
				Name:    StreamOnRelayConnChain,
				Handler: s.onRelayConnChain,
			},
		},
	}
}

func (s *Service) onRouteReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	r := protobuf.NewReader(stream)
	var req pb.RouteReq
	if err = r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: onRouteReq read msg: %s", err.Error())
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(req.Dest)

	s.logger.Tracef("route:%s onRouteReq received: target=%s", s.self.String(), target.String())

	s.metrics.FindRouteReqReceivedCount.Inc()

	var reqPath [][]byte
	for _, v := range req.Paths {
		reqPath = v.Items // request path only one
		if len(reqPath) > int(atomic.LoadInt32(&MaxTTL)) {
			// discard
			s.logger.Tracef("route:%s onRouteReq target=%s discard, ttl=%d", s.self.String(), target.String(), len(reqPath))
			return nil
		}
		if inPath(s.self.Bytes(), reqPath) {
			// discard
			s.logger.Tracef("route:%s onRouteReq target=%s discard, received path contains self.", s.self.String(), target.String())
			return nil
		}
	}
	// passive route save
	s.routeTable.SavePaths(req.Paths)
	s.saveUnderlay(req.UList)

	if s.self.Equal(target) {
		// resp
		s.doRouteResp(ctx, p.Address, target, boson.ZeroAddress, nil, nil, req.UType)
		return nil
	}
	if s.IsNeighbor(target) {
		// dest in neighbor
		s.logger.Tracef("route:%s onRouteReq target=%s in neighbor", s.self.String(), target.String())
		s.doRouteReq(ctx, []boson.Address{target}, p.Address, target, &req, nil)
		return nil
	}

	nowPaths := make([]*Path, 0)
	paths, err := s.GetRoute(ctx, target)
	if err == nil && len(paths) > 0 {
		for _, v := range paths {
			if len(v.Items)+len(reqPath) > int(atomic.LoadInt32(&MaxTTL)) {
				continue
			}
			if !inPaths(reqPath, v.Items) {
				nowPaths = append(nowPaths, v)
			}
		}
		if len(nowPaths) > 0 {
			// have route resp
			switch req.UType {
			case uTypeTarget:
				addr, _ := s.addressbook.Get(target)
				if addr == nil {
					goto FORWARD
				}
			case uTypeZero:
			}
			s.doRouteResp(ctx, p.Address, target, boson.ZeroAddress, nil, nowPaths, req.UType)
			return nil
		}
	}

FORWARD:
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

func (s *Service) onRouteResp(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	r := protobuf.NewReader(stream)
	resp := pb.RouteResp{}
	if err = r.ReadMsgWithContext(ctx, &resp); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteResp read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(resp.Dest)

	now := make([]*pb.Path, 0)
	for _, v := range resp.Paths {
		if len(v.Items) <= int(atomic.LoadInt32(&MaxTTL)) {
			now = append(now, v)
		}
	}
	if len(now) == 0 {
		// discard
		s.logger.Tracef("route:%s onRouteResp target=%s discard, received path length gt max ttl", s.self.String(), target.String())
		return nil
	}
	resp.Paths = now

	s.logger.Tracef("route:%s onRouteResp received: dest= %s", s.self.String(), target.String())

	s.metrics.FindRouteRespReceivedCount.Inc()

	for _, v := range resp.Paths {
		items := v.Items // response path maybe loop back
		if inPath(s.self.Bytes(), items) {
			// discard
			s.logger.Tracef("route:%s onRouteResp target=%s discard, received path contains self.", s.self.String(), target.String())
			return nil
		}
	}
	s.routeTable.SavePaths(resp.Paths)
	s.saveUnderlay(resp.UList)

	// doing forward resp
	s.respForward(ctx, target, peer.Address, &resp)
	return nil
}

func (s *Service) respForward(ctx context.Context, target, last boson.Address, resp *pb.RouteResp) {
	res := s.pendingCalls.Get(target, last)
	skip := make([]boson.Address, 0)
	for _, v := range res {
		if !v.Src.Equal(s.self) {
			if !v.Src.MemberOf(skip) {
				// forward
				s.doRouteResp(ctx, v.Src, target, last, resp, nil)
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
		req.UList = s.convUnderlayList(req.UType, target, src, req.UList)
	} else {
		req = &pb.RouteReq{
			Dest:  target.Bytes(),
			Alpha: NeighborAlpha,
			Paths: s.routeTable.generatePaths(nil),
			UType: uTypeTarget,
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

func (s *Service) doRouteResp(ctx context.Context, src, target, last boson.Address, resp *pb.RouteResp, paths []*Path, uType ...int32) {
	ut := func() int32 {
		if len(uType) > 0 {
			return uType[0]
		}
		return 0
	}
	if resp != nil {
		resp.Paths = s.routeTable.generatePaths(resp.Paths)
		resp.UList = s.convUnderlayList(resp.UType, target, last, resp.UList)
	} else if len(paths) > 0 {
		resp = &pb.RouteResp{
			Dest:  target.Bytes(),
			Paths: s.routeTable.convertPathsToPbPaths(paths),
			UType: ut(),
		}
		resp.UList = s.convUnderlayList(resp.UType, target, last, resp.UList)
	} else {
		resp = &pb.RouteResp{
			Dest:  target.Bytes(),
			Paths: s.routeTable.generatePaths(nil),
			UType: ut(),
		}
	}
	s.sendDataToNode(ctx, src, streamOnRouteResp, resp)
	s.metrics.FindRouteRespSentCount.Inc()
}

func (s *Service) sendDataToNode(ctx context.Context, peer boson.Address, streamName string, msg protobuf.Message) {
	s.logger.Tracef("route:%s sendDataToNode to %s %s", s.self.String(), peer.String(), streamName)
	stream, err1 := s.stream.NewStream(ctx, peer, nil, ProtocolName, ProtocolVersion, streamName)
	if err1 != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: sendDataToNode NewStream, err1=%s", err1)
		return
	}
	w := protobuf.NewWriter(stream)
	err := w.WriteMsgWithContext(ctx, msg)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: sendDataToNode write msg, err=%s", err)
		_ = stream.Reset()
		return
	}
	go stream.FullClose()
}

func (s *Service) getNeighbor(target boson.Address, alpha int32, skip ...boson.Address) (forward []boson.Address) {
	if alpha <= 0 {
		alpha = NeighborAlpha
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
	var direct, notDirect []boson.Address
	for _, v := range now {
		ss := s.kad.SnapshotAddr(v)
		if ss != nil {
			if ss.Reachability == p2p.ReachabilityStatusPublic {
				direct = append(direct, v)
			} else {
				notDirect = append(notDirect, v)
			}
		}
	}
	if len(direct) >= int(alpha) {
		forward, _ = s.kad.RandomSubset(direct, int(alpha))
		return
	}
	forward = append(forward, direct...)

	s.logger.Debugf("target %s getNeighbor limit %d direct %d notDirect %d", target, alpha, len(direct), len(notDirect))

	n := int(alpha) - len(direct)
	if len(notDirect) > n {
		list, _ := s.kad.RandomSubset(notDirect, n)
		forward = append(forward, list...)
		return
	}
	forward = append(forward, notDirect...)
	return
}

func (s *Service) IsNeighbor(dest boson.Address) (has bool) {
	return s.kad.ConnectedPeers().Exists(dest)
}

func (s *Service) GetRoute(_ context.Context, dest boson.Address) ([]*Path, error) {
	return s.routeTable.Get(dest)
}

func (s *Service) FindRoute(ctx context.Context, target boson.Address, timeouts ...time.Duration) (paths []*Path, err error) {
	if s.self.Equal(target) {
		err = fmt.Errorf("target=%s is self", target.String())
		return
	}
	var timeout = findTimeOut
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	errTimeout := fmt.Errorf("find route timeout %.0fs", timeout.Seconds())

	key := "find_route_" + target.String()
	res := make(chan *finRoutePending, 1)
	has, _ := cache.Contains(cacheCtx, key)
	if has {
		s.addFindRoutePending(key, res)
		select {
		case <-ctx.Done():
			err = fmt.Errorf("find route praent ctx %s", ctx.Err())
			s.logger.Debugf("route: FindRoute dest %s %s", target, err)
			return nil, err
		case i := <-res:
			return i.res, i.err
		case <-time.After(timeout):
			s.logger.Debugf("route: FindRoute target %s %s", target, errTimeout)
			return nil, errTimeout
		}
	}

	forward := s.getNeighbor(target, NeighborAlpha, target)
	if len(forward) == 0 {
		s.metrics.TotalErrors.Inc()
		err = fmt.Errorf("find route neighbor notfound")
		s.logger.Errorf("route: FindRoute target %s %s", target, err)
		return nil, err
	}

	_ = cache.Set(cacheCtx, key, 1, timeout)

	ct, cancel := context.WithTimeout(ctx, timeout)
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
		err = errTimeout
		s.logger.Debugf("route: FindRoute target %s %s", target, errTimeout)
	case <-ctx.Done():
		remove()
		err = fmt.Errorf("praent ctx %s", ctx.Err())
		s.logger.Debugf("route: FindRoute dest %s %s", target, err)
	case <-resCh:
		paths, err = s.GetRoute(ctx, target)
	}

	_, _ = cache.Remove(cacheCtx, key)

	out := &finRoutePending{
		err: err,
		res: paths,
	}
	go s.returnFindRoutePending(key, out)
	return paths, err
}

func (s *Service) addFindRoutePending(key string, res chan *finRoutePending) {
	s.findRouteMix.Lock()
	defer s.findRouteMix.Unlock()

	resChs, ok := s.findRouteCall[key]
	if ok {
		s.findRouteCall[key] = append(resChs, res)
	} else {
		s.findRouteCall[key] = []chan *finRoutePending{res}
	}
}

func (s *Service) returnFindRoutePending(key string, res *finRoutePending) {
	s.findRouteMix.Lock()
	chs := s.findRouteCall[key]
	delete(s.findRouteCall, key)
	s.findRouteMix.Unlock()

	for _, v := range chs {
		select {
		case v <- res:
		default:
		}
	}
}

func (s *Service) DelRoute(_ context.Context, target boson.Address) error {
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
	var logContent string
	for _, v := range addresses {
		logContent += v.String() + "\n"
	}
	s.logger.Tracef("get dest=%s neighbor \n%s", target, logContent)
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

func (s *Service) isConnected(_ context.Context, target boson.Address) bool {
	var isConnected bool
	findFun := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if target.Equal(address) {
			isConnected = true
			return true, false, nil
		}
		return false, false, nil
	}
	_ = s.kad.EachPeer(findFun, topology.Filter{Reachable: false})
	if isConnected {
		s.logger.Tracef("route: connect target %s in neighbor", target)
		return true
	}
	_ = s.lightNodes.EachPeer(findFun)
	if isConnected {
		s.logger.Tracef("route: connect target(light) %s in neighbor", target)
		return true
	}
	return false
}

func (s *Service) connect(ctx context.Context, peer boson.Address) (err error) {
	addr, err := s.kad.GetAuroraAddress(peer)
	if err != nil {
		addr, err = s.FindUnderlay(ctx, peer)
		if err != nil {
			return err
		}
	}
	return s.kad.Connection(ctx, addr)
}

func (s *Service) getOrFindRoute(ctx context.Context, target boson.Address) (paths []*Path, err error) {
	paths, err = s.GetRoute(ctx, target)
	if errors.Is(err, ErrNotFound) {
		paths, err = s.FindRoute(ctx, target)
	}
	return
}

func (s *Service) FindUnderlay(ctx context.Context, target boson.Address, timeouts ...time.Duration) (addr *aurora.Address, err error) {
	stream, err := s.stream.NewRelayStream(ctx, target, nil, ProtocolName, ProtocolVersion, streamOnFindUnderlay, true)
	if err != nil {
		return nil, err
	}
	var timeout time.Duration
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	} else {
		timeout = time.Second * 3
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	start := time.Now()
	defer func() {
		cancel()
		if err != nil {
			_ = stream.Reset()
		} else {
			s.logger.Infof("find underlay dest %s successful %s", target.String(), time.Since(start))
			_ = stream.Close()
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

	resp := &pb.UnderlayResp{}
	if err = r.ReadMsgWithContext(ctx, resp); err != nil {
		s.logger.Errorf("find underlay dest %s read msg: %s", target, err.Error())
		return nil, err
	}

	addr, err = aurora.ParseAddress(resp.Underlay, resp.Dest, resp.Signature, s.networkID)
	if err != nil {
		s.logger.Errorf("find underlay dest %s parse err %s", target.String(), err.Error())
		return nil, err
	}
	err = s.addressbook.Put(addr.Overlay, *addr)
	if err != nil {
		return nil, err
	}
	return addr, nil
}

func (s *Service) onFindUnderlay(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	req := pb.UnderlayReq{}
	if err = r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: onFindUnderlay read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	target := boson.NewAddress(req.Dest)
	s.logger.Tracef("find underlay dest %s receive: from %s", target.String(), p.Address.String())
	address, err := s.addressbook.Get(target)
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
	return err
}

func (s *Service) GetNextHopRandomOrFind(ctx context.Context, target boson.Address, skips ...boson.Address) (next boson.Address, err error) {
	next = s.getNextHopRandom(target, skips...)
	if next.IsZero() {
		_, err = s.FindRoute(ctx, target)
		if err != nil {
			return
		}
		next = s.getNextHopRandom(target, skips...)
		if next.IsZero() {
			err = fmt.Errorf("nexthop not found")
			return
		}
	}
	return
}

func (s *Service) getNextHopRandom(target boson.Address, skips ...boson.Address) (next boson.Address) {
	list := s.getNextHopEffective(target, skips...)
	if len(list) > 0 {
		k := rand.Intn(len(list))
		s.routeTable.updateUsedTime(target, list[k])
		return list[k]
	}
	return boson.ZeroAddress
}

func (s *Service) getNextHopEffective(target boson.Address, skips ...boson.Address) (next []boson.Address) {
	list := s.routeTable.GetNextHop(target, skips...)
	for _, v := range list {
		if s.IsNeighbor(v) {
			next = append(next, v)
		}
	}
	return next
}

func (s *Service) onRelay(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	var (
		target        boson.Address
		next          boson.Address
		forwardStream p2p.Stream
		forwardWriter protobuf.Writer
		forwardReader protobuf.Reader
		forwardNext   bool
		quit          bool
	)

	errChan := make(chan error, 1)
	forwardChan := make(chan *pb.RouteRelayReq, 1)
	readRespChan := make(chan *pb.RouteRelayResp, 1)

	defer func() {
		if forwardStream != nil {
			quit = true
			_ = forwardStream.Reset()
		}
	}()

	req, w, r, forwardNext, err := s.p2ps.CallHandler(ctx, p, stream)

	if forwardNext {
		forwardChan <- req
	} else {
		return err
	}

	// read forward response
	readResp := func(ch chan *pb.RouteRelayResp) {
		for {
			resp := &pb.RouteRelayResp{}
			e := forwardReader.ReadMsg(resp)
			if quit {
				return
			}
			switch e {
			case context.Canceled, io.EOF:
				// when next node FullClose
				errChan <- nil
				return
			case nil:
				ch <- resp
				s.logger.Tracef("route: onRelay target %s receive: from %s", target, next)
			default:
				content := fmt.Sprintf("route: onRelay read resp from next: %s", e.Error())
				s.logger.Debug(content)
				errChan <- fmt.Errorf(content)
				return
			}
		}
	}

	for {
		select {
		case err = <-r.Err:
			switch err {
			case context.Canceled, io.EOF:
				return nil
			default:
				return err
			}
		case req := <-forwardChan:
			req.Paths = append(req.Paths, s.self.Bytes())
			target = boson.NewAddress(req.Dest)
			// jump next
			if forwardStream == nil {
				if s.IsNeighbor(target) {
					next = target
					s.logger.Tracef("route: onRelay the path has %d jump", len(req.Paths))
				} else {
					_, skips := generatePathItems(req.Paths)
					next, err = s.GetNextHopRandomOrFind(ctx, target, skips...)
					if err != nil {
						s.logger.Debugf("route: onRelay target %s nextHop not found", target)
						errChan <- err
						break
					}
				}
				forwardStream, err = s.stream.NewStream(ctx, next, stream.Headers(), ProtocolName, ProtocolVersion, StreamOnRelay)
				if err != nil {
					errChan <- err
					break
				}
				forwardWriter, forwardReader = protobuf.NewWriterAndReader(forwardStream)
				go readResp(readRespChan)
				s.logger.Tracef("route: relay stream created, target %s next %s", target, next)
			}
			if err = forwardWriter.WriteMsg(req); err != nil {
				content := fmt.Sprintf("route: onRelay forward msg: %s", err.Error())
				s.logger.Errorf(content)
				errChan <- fmt.Errorf(content)
				break
			}
			s.logger.Tracef("route: onRelay target %s forward req to %s", target, next)
		case resp := <-readRespChan:
			w.W <- resp.Data
			err = <-w.Err
			if err != nil {
				content := fmt.Sprintf("route: onRelay target %s send resp to %s: %s", target, p.Address, err.Error())
				s.logger.Errorf(content)
				errChan <- fmt.Errorf(content)
				break
			}
			s.logger.Tracef("route: onRelay target %s send resp to %s", target, p.Address)
		case err = <-errChan:
			return err
		}
	}
}

// PackRelayReq This packet mode is UDP mode and writing success does not mean that the final target has received a message
func (s *Service) PackRelayReq(ctx context.Context, stream p2p.VirtualStream, req *pb.RouteRelayReq) {
	go func() {
		var quit bool
		w, r := protobuf.NewWriterAndReader(stream.RealStream())
		var err error
		defer func() {
			stream.UpdateStatRealStreamClosed()
			quit = true
			_ = stream.RealStream().Reset()
		}()
		go func() {
			defer stream.UpdateStatRealStreamClosed()
			for {
				resp := &pb.RouteRelayResp{}
				err = r.ReadMsg(resp)
				if err != nil {
					if quit {
						err = nil
					}
					stream.Reader().Err <- err
					return
				}
				stream.Reader().R <- resp.Data
			}
		}()
		for {
			select {
			case <-stream.Done():
				return
			case <-ctx.Done():
				return
			case p := <-stream.Writer().W:
				req.Data = p
				err = w.WriteMsg(req)
				stream.Writer().Err <- err
				if err != nil {
					return
				}
			}
		}
	}()
}

// PackRelayResp This packet mode is UDP mode and writing success does not mean that the final target has received a message
// This channel is closed by the initiator node
func (s *Service) PackRelayResp(ctx context.Context, stream p2p.VirtualStream, reqCh chan *pb.RouteRelayReq) {
	go func() {
		var quit bool
		w, r := protobuf.NewWriterAndReader(stream.RealStream())
		var err error
		defer func() {
			stream.UpdateStatRealStreamClosed()
			quit = true
			_ = stream.RealStream().Reset()
		}()

		first := true

		go func() {
			defer stream.UpdateStatRealStreamClosed()
			for {
				req := &pb.RouteRelayReq{}
				err = r.ReadMsg(req)
				if first {
					first = false
					if err != nil {
						reqCh <- nil
					} else {
						reqCh <- req
					}
				}
				if err != nil {
					if quit {
						err = nil
					}
					stream.Reader().Err <- err
					return
				}
				stream.Reader().R <- req.Data
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-stream.Writer().W:
				err = w.WriteMsg(&pb.RouteRelayResp{Data: p})
				stream.Writer().Err <- err
				if err != nil {
					return
				}
			}
		}
	}()
}

func (s *Service) convUnderlayList(uType int32, target, last boson.Address, old []*pb.UnderlayResp) (out []*pb.UnderlayResp) {
	switch uType {
	case uTypeTarget:
		if target.Equal(last) {
			addr, _ := s.addressbook.Get(target)
			if addr != nil {
				out = []*pb.UnderlayResp{{
					Dest:      target.Bytes(),
					Underlay:  addr.Underlay.Bytes(),
					Signature: addr.Signature,
				}}
				return
			}
		}
		return old
	}
	return
}

func (s *Service) saveUnderlay(uList []*pb.UnderlayResp) {
	for _, v := range uList {
		addr, err := aurora.ParseAddress(v.Underlay, v.Dest, v.Signature, s.networkID)
		if err != nil {
			s.logger.Errorf("route: parse aurora address %s", err.Error())
		} else {
			err = s.addressbook.Put(addr.Overlay, *addr)
			if err != nil {
				s.logger.Errorf("route: address book put %s", err.Error())
			}
		}
	}
}

func (s *Service) onRelayConnChain(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			s.logger.Tracef("onRelayConnChain from %s err %s", p.Address, err)
			_ = stream.Reset()
		} else {
			_ = stream.Close()
			s.logger.Tracef("onRelayConnChain from %s stream close", p.Address)
		}
	}()

	var req pb.RouteRelayReq
	r := protobuf.NewReader(stream)
	err = r.ReadMsgWithContext(ctx, &req)
	if err != nil {
		return fmt.Errorf("read syn err %s", err)
	}

	req.Paths = append(req.Paths, s.self.Bytes())
	target := boson.NewAddress(req.Dest)
	if target.Equal(s.self) {
		md, err := aurora.NewModelFromBytes(req.SrcMode)
		if err != nil {
			return err
		}
		src := p2p.Peer{
			Address: boson.NewAddress(req.Src),
			Mode:    md,
		}
		return s.p2ps.CallHandlerWithConnChain(ctx, p, src, stream, string(req.ProtocolName), string(req.ProtocolVersion), string(req.StreamName))
	}

	var next boson.Address
	if s.IsNeighbor(target) {
		next = target
		s.logger.Tracef("route: onRelayConnChain the path has %d jump", len(req.Paths))
	} else {
		_, skips := generatePathItems(req.Paths)
		next, err = s.GetNextHopRandomOrFind(ctx, target, skips...)
		if err != nil {
			return err
		}
	}

	remoteConn, err := s.stream.NewStream(ctx, next, stream.Headers(), ProtocolName, ProtocolVersion, StreamOnRelayConnChain)
	if err != nil {
		return fmt.Errorf("new forward stream to %s %s", next, err)
	}
	defer remoteConn.Close()

	w := protobuf.NewWriter(remoteConn)
	err = w.WriteMsgWithContext(ctx, &req)
	if err != nil {
		return fmt.Errorf("forward syn err %s", err)
	}
	// response
	respErrCh := make(chan error, 1)
	go func() {
		_, err = io.Copy(stream, remoteConn)
		s.logger.Tracef("route: onRelayConnChain io.copy resp err %v", err)
		respErrCh <- err
	}()
	// request
	reqErrCh := make(chan error, 1)
	go func() {
		_, err = io.Copy(remoteConn, stream)
		s.logger.Tracef("route: onRelayConnChain io.copy req err %v", err)
		reqErrCh <- err
	}()
	select {
	case err = <-respErrCh:
		return err
	case err = <-reqErrCh:
		return err
	}
}
