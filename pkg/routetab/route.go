package routetab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gogf/gf/os/gmlock"
	"time"
)

const (
	protocolName        = "router"
	protocolVersion     = "1.0.0"
	streamFindRouteReq  = "FindRouteReq"
	streamFindRouteResp = "FindRouteResp"
)

var (
	ErrNotFound           = errors.New("route: not found")
	NeighborAlpha         = 2
	MaxTTL          uint8 = 7
	GcTime                = time.Minute * 10
	GcInterval            = time.Minute
	PendingTimeout        = time.Second * 10
	PendingInterval       = time.Second
)

type RouteTab interface {
	GetRoute(ctx context.Context, dest boson.Address) (neighbor boson.Address, err error)
	FindRoute(ctx context.Context, dest boson.Address) (route RouteItem, err error)
}

// RouteItem
///									                  |  -- (nextHop)
///									   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
///      neighbor <--|
///					 |				                  |  -- (nextHop)
///					 |				   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
type RouteItem struct {
	CreateTime int64
	TTL        uint8
	Neighbor   boson.Address
	NextHop    []RouteItem
}

type routeTable struct {
	prefix      string
	mu          *gmlock.Locker
	store       storage.StateStorer
	logger      logging.Logger
	lockTimeout time.Duration
}

func newRouteTable(store storage.StateStorer, logger logging.Logger) *routeTable {
	return &routeTable{
		prefix:      protocolName,
		mu:          gmlock.New(),
		store:       store,
		logger:      logger,
		lockTimeout: time.Second * 5,
	}
}

func (rt *routeTable) tryLock(key string) error {
	now := time.Now()
	for !rt.mu.TryRLock(key) {
		time.After(time.Millisecond * 50)
		if time.Since(now).Seconds() > rt.lockTimeout.Seconds() {
			rt.logger.Errorf("routeTable: %s try lock timeout", key)
			err := fmt.Errorf("try lock timeout")
			return err
		}
	}
	return nil
}

func (rt *routeTable) Set(target boson.Address, routes []RouteItem) error {
	dest := target.String()
	key := rt.prefix + dest
	err := rt.tryLock(key)
	if err != nil {
		return err
	}
	defer rt.mu.RUnlock(key)

	old := make([]RouteItem, 0)
	err = rt.store.Get(key, &old)
	if err != nil && err != ErrNotFound {
		err = fmt.Errorf("routeTable: Set %s store get error: %s", dest, err.Error())
		rt.logger.Errorf(err.Error())
		return err
	}
	if len(old) > 0 {
		routes = mergeRouteList(routes, old)
	}

	err = rt.store.Put(key, routes)
	if err != nil {
		rt.logger.Errorf("routeTable: Set %s store put error: %s", dest, err.Error())
	}
	return err
}

func (rt *routeTable) Get(target boson.Address) (routes []RouteItem, err error) {
	dest := target.String()
	key := rt.prefix + dest
	err = rt.tryLock(key)
	if err != nil {
		return
	}
	defer rt.mu.RUnlock(key)

	err = rt.store.Get(key, &routes)
	if err != nil {
		if err == storage.ErrNotFound {
			err = ErrNotFound
			return
		}
		err = fmt.Errorf("routeTable: Get %s store get error: %s", dest, err.Error())
		rt.logger.Errorf(err.Error())
	}
	return
}

func (rt *routeTable) Gc(expire time.Duration) {
	err := rt.store.Iterate(rt.prefix, func(target, value []byte) (stop bool, err error) {
		key := string(target)
		err = rt.tryLock(key)
		if err != nil {
			return false, err
		}
		defer rt.mu.RUnlock(key)
		routes := make([]RouteItem, 0)
		err = json.Unmarshal(value, &routes)
		if err != nil {
			return false, err
		}
		now, updated := checkExpired(routes, expire)
		if updated {
			if len(now) > 0 {
				err = rt.store.Put(key, now)
				if err != nil {
					return false, err
				}
			} else {
				err = rt.store.Delete(key)
				if err != nil {
					return false, err
				}
			}

		}
		return false, nil
	})
	if err != nil {
		rt.logger.Errorf("routeTable: gc err %s", err)
	}
}

type pendCallResItem struct {
	src        boson.Address
	createTime time.Time
	resCh      chan struct{}
}

type pendingCallResArray []pendCallResItem
type pendCallResTab map[common.Hash]pendingCallResArray

type Service struct {
	addr         boson.Address
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	pendingCalls pendCallResTab
	routeTable   *routeTable
	kad          *kademlia.Kad
}

func New(addr boson.Address, ctx context.Context, streamer p2p.Streamer, kad *kademlia.Kad, store storage.StateStorer, logger logging.Logger) Service {
	// load route table from db only those valid item will be loaded

	service := Service{
		addr:         addr,
		streamer:     streamer,
		logger:       logger,
		kad:          kad,
		pendingCalls: pendCallResTab{},
		routeTable:   newRouteTable(store, logger),
		metrics:      newMetrics(),
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
		s.routeTable.Gc(GcTime)
		ticker := time.NewTicker(GcInterval)
		for {
			<-ticker.C
			s.routeTable.Gc(GcTime)
		}
	}()
	go s.pendingClean()
	select {
	case <-ctx.Done():
		return
	}
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamFindRouteReq,
				Handler: s.handlerFindRouteReq,
			},
			{
				Name:    streamFindRouteResp,
				Handler: s.handlerFindRouteResp,
			},
		},
	}
}

func (s *Service) handlerFindRouteReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer func() {
		err := stream.FullClose()
		if err != nil {
			s.logger.Warningf("route: handlerFindRouteReq stream.FullClose: %s", err.Error())
		}
	}()

	var req pb.FindRouteReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteReq read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("route: handlerFindRouteReq received: dest= %s", boson.NewAddress(req.Dest).String())

	// passive route save
	go func(path [][]byte) {
		for i, target := range path {
			now := pathToRouteItem(path[i:])
			_ = s.routeTable.Set(boson.NewAddress(target), now)
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
				forward := s.getNeighbor()
				for _, v := range forward {
					if !inPath(v.Bytes(), req.Path) {
						// forward
						req.Path = append(req.Path, p.Address.Bytes())
						s.doReq(ctx, p.Address, p2p.Peer{Address: v}, dest, &req, nil)
						s.logger.Tracef("route: handlerFindRouteReq dest= %s forward ro %s", dest.String(), v.String())
					}
					// discard
					s.logger.Tracef("route: handlerFindRouteReq dest= %s discard", dest.String())
				}
			} else {
				// dest in neighbor then resp
				nowPath := [][]byte{dest.Bytes(), s.addr.Bytes()}
				routes = pathToRouteItem(nowPath)
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

func (s *Service) handlerFindRouteResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer func() {
		err := stream.FullClose()
		if err != nil {
			s.logger.Warningf("route: handlerFindRouteResp stream.FullClose: %s", err.Error())
		}
	}()

	resp := pb.FindRouteResp{}
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		content := fmt.Sprintf("route: handlerFindRouteResp read msg: %s", err.Error())
		s.logger.Errorf(content)
		return fmt.Errorf(content)
	}
	s.logger.Tracef("route: handlerFindRouteResp received: dest= %s", boson.NewAddress(resp.Dest).String())

	go s.saveRespRouteItem(ctx, p.Address, resp)
	return nil
}

func (s *Service) FindRoute(ctx context.Context, dest boson.Address) (routes []RouteItem, err error) {
	routes, err = s.GetRoute(ctx, dest)
	if err != nil {
		s.logger.Debugf("route: FindRoute dest %s", dest.String(), err)
		if s.isNeighbor(dest) {
			err = fmt.Errorf("route: FindRoute dest %s is neighbor", dest.String())
			return
		}
		forward := s.getNeighbor()
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
				err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", dest.String(), PendingTimeout.Seconds())
			case <-resCh:
				routes, err = s.GetRoute(ctx, dest)
			}
			return
		}
		s.logger.Errorf("route: FindRoute dest %s , neighbor len equal 0", dest.String())
		err = fmt.Errorf("neighbor len equal 0")
	}
	return
}

func (s *Service) GetRoute(_ context.Context, dest boson.Address) (routes []RouteItem, err error) {
	return s.routeTable.Get(dest)
}

func (s *Service) pendingClean() {
	ticker := time.NewTicker(PendingInterval)
	for {
		<-ticker.C
		for destKey, item := range s.pendingCalls {
			now := pendingCallResArray{}
			for _, v := range item {
				if time.Since(v.createTime).Seconds() < PendingTimeout.Seconds() {
					now = append(now, v)
				}
			}
			if len(now) == 0 {
				delete(s.pendingCalls, destKey)
			} else {
				s.pendingCalls[destKey] = now
			}
		}
	}
}

func (s *Service) saveRespRouteItem(ctx context.Context, neighbor boson.Address, resp pb.FindRouteResp) {
	minTTL := MaxTTL
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

	destKey := common.BytesToHash(resp.Dest)

	_ = s.routeTable.Set(boson.NewAddress(resp.Dest), now)

	// doing resp
	res, has := s.pendingCalls[destKey]
	if has {
		delete(s.pendingCalls, destKey)
		for _, v := range res {
			if !v.src.Equal(s.addr) {
				// forward
				dest := boson.NewAddress(resp.Dest)
				s.doResp(ctx, p2p.Peer{Address: v.src}, dest, now)
			} else if v.resCh != nil {
				// sync return
				v.resCh <- struct{}{}
			}
		}
	}
}

func (s *Service) getNeighbor() (forward []boson.Address) {
	forward = make([]boson.Address, 0)
	cnt := 0
	err := s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if u >= s.kad.NeighborhoodDepth() {
			// neighbor
			forward = append(forward, address)
			cnt++
			if cnt >= NeighborAlpha {
				return true, false, nil
			}
		}
		return false, true, nil
	})
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: get neighbor: %s", err.Error())
	}
	return
}

func (s *Service) isNeighbor(dest boson.Address) (has bool) {
	err := s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
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

func (s *Service) doReq(ctx context.Context, src boson.Address, peer p2p.Peer, dest boson.Address, req *pb.FindRouteReq, ch chan struct{}) {
	s.logger.Tracef("route: doReq dest %s to neighbor %s", dest.String(), peer.Address.String())
	stream, err1 := s.streamer.NewStream(ctx, peer.Address, nil, protocolName, protocolVersion, streamFindRouteReq)
	if err1 != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doReq NewStream: err1=%s", err1)
		return
	}
	defer func() {
		err := stream.FullClose()
		if err != nil {
			s.logger.Warningf("route: doReq stream.FullClose: %s", err.Error())
		}
	}()

	s.pendingAdd(dest, src, ch)

	w := protobuf.NewWriter(stream)
	err := w.WriteMsgWithContext(ctx, req)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doReq write msg: err=%s", err)
	}
}

func (s *Service) pendingAdd(dest, src boson.Address, ch chan struct{}) {
	pending := pendCallResItem{
		src:        src,
		createTime: time.Now(),
		resCh:      ch,
	}
	destKey := common.BytesToHash(dest.Bytes())
	_, has := s.pendingCalls[destKey]
	if !has {
		s.pendingCalls[destKey] = pendingCallResArray{pending}
	} else {
		s.pendingCalls[destKey] = append(s.pendingCalls[destKey], pending)
	}
}

func (s *Service) doResp(ctx context.Context, peer p2p.Peer, dest boson.Address, routes []RouteItem) {
	s.logger.Tracef("route: doResp dest %s to neighbor %s", dest.String(), peer.Address.String())
	stream, err1 := s.streamer.NewStream(ctx, peer.Address, nil, protocolName, protocolVersion, streamFindRouteResp)
	if err1 != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doResp NewStream: err1=%s", err1)
		return
	}
	defer func() {
		err := stream.FullClose()
		if err != nil {
			s.logger.Warningf("route: doResp stream.FullClose: %s", err.Error())
		}
	}()
	resp := &pb.FindRouteResp{
		Dest:       dest.Bytes(),
		RouteItems: convRouteToPbRouteList(routes),
	}
	w := protobuf.NewWriter(stream)
	err := w.WriteMsgWithContext(ctx, resp)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doResp write msg: err=%s", err)
	}
}
