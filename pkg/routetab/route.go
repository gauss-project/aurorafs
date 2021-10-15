package routetab

import (
	"context"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto/bls"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"time"
)

const (
	protocolName         = "router"
	protocolVersion      = "2.0.0"
	streamOnRouteReq     = "onRouteReq"
	streamOnRouteResp    = "onRouteResp"
	streamOnFindUnderlay = "onFindUnderlay"

	peerConnectionAttemptTimeout = 5 * time.Second // Timeout for establishing a new connection with peer.
)

var (
	errOverlayMismatch = errors.New("overlay mismatch")
	errPruneEntry      = errors.New("prune entry")

	MaxTTL               uint8 = 10
	DefaultNeighborAlpha int32 = 2
	gcTime                     = time.Minute * 10
	gcInterval                 = time.Minute
)

type RouteTab interface {
	GetRoute(ctx context.Context, dest boson.Address) (paths []*Path, err error)
	FindRoute(ctx context.Context, dest boson.Address) (paths []*Path, err error)
	DelRoute(ctx context.Context, dest boson.Address) (err error)
	Connect(ctx context.Context, dest boson.Address) error
	GetTargetNeighbor(ctx context.Context, dest boson.Address, limit int) (addresses []boson.Address, err error)
	IsNeighbor(dest boson.Address) (has bool)
}

type Service struct {
	self         boson.Address
	p2ps         p2p.Service
	logger       logging.Logger
	metrics      metrics
	pendingCalls *pendCallResTab
	routeTable   *Table
	kad          *kademlia.Kad
	config       Config
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
	s.routeTable.ResumeSigned()
	s.routeTable.ResumeRoute()

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
			{
				Name:    streamOnFindUnderlay,
				Handler: s.onFindUnderlay,
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
	err := s.routeTable.ReqToSave(&req)
	if err != nil {
		s.logger.Errorf("route: target=%s reqToSave %s", target.String(), err.Error())
	}

	if len(req.Path.Item) > int(MaxTTL) {
		// discard
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s discard, ttl=%d", s.self.String(), target.String(), len(req.Path.Item))
		return nil
	}
	if inPath(s.self.Bytes(), req.Path.Item) {
		// discard
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s discard, received path contains self.", s.self.String(), target.String())
		return nil
	}
	if s.self.Equal(target) {
		// resp
		s.doRouteResp(ctx, p.Address, s.routeTable.ReqToResp(&req, nil))
		return nil
	}
	if s.IsNeighbor(target) {
		// dest in neighbor
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s in neighbor", s.self.String(), target.String())
		s.doRouteReq(ctx, target, p.Address, target, &req, nil)
		return nil
	}

	paths, err := s.GetRoute(ctx, target)
	if err == nil {
		// have route resp
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s in route table", s.self.String(), target.String())
		s.doRouteResp(ctx, p.Address, s.routeTable.ReqToResp(&req, paths))
		return nil
	}

	// forward
	skip := make([]boson.Address, 0)
	for _, v := range req.Path.Item {
		skip = append(skip, boson.NewAddress(v))
	}
	forward := s.getNeighbor(target, req.Alpha, skip...)
	for _, next := range forward {
		// forward
		s.doRouteReq(ctx, next, p.Address, target, &req, nil)
		s.logger.Tracef("route:%s handlerFindRouteReq target=%s forward to %s", s.self.String(), target.String(), next.String())
	}
	return nil
}

func (s *Service) onRouteResp(ctx context.Context, _ p2p.Peer, stream p2p.Stream) error {
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

	err := s.routeTable.RespToSave(&resp)
	if err != nil {
		s.logger.Errorf("route: target=%s respToSave %s", target.String(), err.Error())
		return err
	}
	// doing forward resp
	s.respForward(ctx, target, &resp)
	return nil
}

func (s *Service) respForward(ctx context.Context, target boson.Address, resp *pb.RouteResp) {
	res := s.pendingCalls.Get(target)
	if res == nil {
		return
	}
	for _, v := range res {
		if !v.Src.Equal(s.self) {
			// forward
			s.doRouteResp(ctx, v.Src, resp)
		} else if v.ResCh != nil {
			// sync return
			v.ResCh <- struct{}{}
		}
	}
}

func (s *Service) doRouteReq(ctx context.Context, peer, src, dest boson.Address, req *pb.RouteReq, ch chan struct{}) {
	if req != nil {
		// forward add sign
		req.Path.Sign = bls.Sign(s.self.Bytes()) // todo
		req.Path.Item = append(req.Path.Item, s.self.Bytes())
	} else {
		req = &pb.RouteReq{
			Dest:  dest.Bytes(),
			Alpha: DefaultNeighborAlpha,
			Path: &pb.Path{
				Sign: bls.Sign(s.self.Bytes()), // todo
				Item: [][]byte{s.self.Bytes()},
			},
		}
	}
	has := s.pendingCalls.Add(boson.NewAddress(req.Dest), src, ch)
	if !has {
		s.sendDataToNode(ctx, peer, streamOnRouteReq, req)
		s.metrics.FindRouteReqSentCount.Inc()
	}
}

func (s *Service) doRouteResp(ctx context.Context, peer boson.Address, resp *pb.RouteResp) {
	s.sendDataToNode(ctx, peer, streamOnRouteResp, resp)
	s.metrics.FindRouteRespSentCount.Inc()
}

func (s *Service) sendDataToNode(ctx context.Context, peer boson.Address, streamName string, msg protobuf.Message) {
	s.logger.Tracef("route:%s sendDataToNode to %s %s", s.self.String(), peer.String(), streamName)
	stream, err1 := s.config.Stream.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
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
	paths, err := s.routeTable.Get(dest)
	if err != nil {
		return nil, err
	}
	out := make([]*Path, 0)
	for _, v := range paths {
		if !s.IsNeighbor(v.Item[1]) {
			s.routeTable.Delete(v)
		} else {
			out = append(out, v)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	return nil, ErrNotFound
}

func (s *Service) FindRoute(ctx context.Context, target boson.Address) (paths []*Path, err error) {
	if s.self.Equal(target) {
		err = fmt.Errorf("target=%s is self", target.String())
		return
	}
	if s.IsNeighbor(target) {
		err = fmt.Errorf("target=%s is neighbor", target.String())
		return
	}
	forward := s.getNeighbor(target, DefaultNeighborAlpha)
	if len(forward) > 0 {
		ct, cancel := context.WithTimeout(ctx, PendingTimeout)
		defer cancel()
		resCh := make(chan struct{}, len(forward))
		for _, next := range forward {
			s.doRouteReq(ct, next, s.self, target, nil, resCh)
		}
		select {
		case <-ct.Done():
			s.pendingCalls.Delete(target)
			err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", target.String(), PendingTimeout.Seconds())
			s.logger.Debugf(err.Error())
		case <-ctx.Done():
			s.pendingCalls.Delete(target)
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
	var routes []*Path
	routes, err = s.GetRoute(ctx, target)
	if errors.Is(err, ErrNotFound) {
		routes, err = s.FindRoute(ctx, target)
		if err != nil {
			return
		}
	}
	if err != nil {
		return
	}
	addresses = GetClosestNeighborLimit(target, routes, limit)
	for _, v := range addresses {
		s.logger.Debugf("get dest=%s neighbor %v", target, v.String())
	}
	return
}

func (s *Service) Connect(ctx context.Context, target boson.Address) error {
	if target.Equal(s.self) {
		return errors.New("cannot connected to self")
	}
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
		return nil
	}
	_ = s.config.LightNodes.EachPeer(findFun)
	if isConnected {
		return nil
	}
	return s.connect(ctx, target)
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
		auroraAddr, err = s.findUnderlay(ctx, peer)
		if err != nil {
			auroraAddr, err = s.findUnderlay(ctx, peer)
			if err != nil {
				return err
			}
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
		s.logger.Debugf("route: could not connect to peer %s: %v", peer, err)
		s.metrics.TotalOutboundConnectionFailedAttempts.Inc()
		remove(peer)
		return err
	case !i.Overlay.Equal(peer):
		_ = s.p2ps.Disconnect(peer, errOverlayMismatch.Error())
		_ = s.p2ps.Disconnect(i.Overlay, errOverlayMismatch.Error())
		return errOverlayMismatch
	}

	s.metrics.TotalOutboundConnections.Inc()
	s.kad.KnownPeers().Add(peer)

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
	next := s.routeTable.GetNextHop(target, req.Sign)
	if next.IsZero() {
		err = errors.New("route expired")
		return
	}
	stream2, err := s.config.Stream.NewStream(ctx, next, nil, protocolName, protocolVersion, streamOnFindUnderlay)
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
	s.logger.Tracef("find underlay dest %s forward: to %s", target.String(), next.String())
	resp := &pb.UnderlayResp{}
	if err = r2.ReadMsgWithContext(ctx, resp); err != nil {
		content := fmt.Sprintf("route: onFindUnderlay read resp msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("find underlay dest %s receive2: from %s", target.String(), next.String())

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

func (s *Service) findUnderlay(ctx context.Context, target boson.Address) (addr *aurora.Address, err error) {
	paths, err := s.GetRoute(ctx, target)
	if errors.Is(err, ErrNotFound) {
		paths, err = s.FindRoute(ctx, target)
		if err != nil {
			return
		}
	}

	for _, path := range paths {
		addr, err = s.readStream(ctx, target, path)
		if err == nil {
			return
		}
	}
	return
}

func (s *Service) readStream(ctx context.Context, target boson.Address, path *Path) (*aurora.Address, error) {
	stream, err := s.config.Stream.NewStream(ctx, path.Item[1], nil, protocolName, protocolVersion, streamOnFindUnderlay)
	if err != nil {
		// delete invalid path
		s.routeTable.Delete(path)
		return nil, err
	}
	path.UsedTime = time.Now()

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
		Sign: path.Sign,
	}
	if err = w.WriteMsgWithContext(ctx, &req); err != nil {
		s.logger.Errorf("find underlay dest %s req err %s", target.String(), err.Error())
		return nil, err
	}
	s.logger.Tracef("find underlay dest %s req to %s", target.String(), path.Item[1].String())

	resp := &pb.UnderlayResp{}
	if err = r.ReadMsgWithContext(ctx, resp); err != nil {
		s.logger.Errorf("find underlay dest %s read msg: %s", target.String(), err.Error())
		return nil, err
	}
	s.logger.Tracef("find underlay dest %s receive: from %s", target.String(), path.Item[1].String())

	var addr *aurora.Address
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
