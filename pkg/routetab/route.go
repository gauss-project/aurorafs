package routetab

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
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
	maxTTL         uint8 = 7
	burnTime             = time.Minute * 10
	pendingTimeout       = time.Second * 10
	neighborAlpha        = 2
	burnTickTime         = time.Minute
	pendTickTime         = time.Second * 5
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
	createTime int64
	ttl        uint8
	neighbor   boson.Address
	nextHop    []RouteItem
}

type RouteTable struct {
	items map[common.Hash][]RouteItem
	gmlock.Locker
}

type pendCallResItem struct {
	src        boson.Address
	createTime time.Time
	resCh      chan []RouteItem
}

type pendingCallResArray []pendCallResItem
type pendCallResTab map[common.Hash]pendingCallResArray

type respChan struct {
	neighbor boson.Address
	pb       pb.FindRouteResp
}

type Service struct {
	addr         boson.Address
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	pendingCalls pendCallResTab
	routeTable   RouteTable
	kad          *kademlia.Kad
	resCh        chan respChan
	reqCh        chan [][]byte // req path to save route
}

func New(addr boson.Address, ctx context.Context, streamer p2p.Streamer, kad *kademlia.Kad, logger logging.Logger) Service {
	// load route table from db only those valid item will be loaded

	service := Service{
		addr:         addr,
		streamer:     streamer,
		logger:       logger,
		kad:          kad,
		pendingCalls: pendCallResTab{},
		routeTable:   RouteTable{items: make(map[common.Hash][]RouteItem)},
		resCh:        make(chan respChan, 1000),
		reqCh:        make(chan [][]byte, 1000),
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
	go s.burn()
	go s.pendingClean()
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-s.resCh:
			// save resp route
			s.saveRespRouteItem(ctx, resp)
		case req := <-s.reqCh:
			// save req route
			s.saveReqRouteItem(req)
		}
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
	s.reqCh <- req.Path

	dest := boson.NewAddress(req.Dest)

	if len(req.Path) <= int(maxTTL) {
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
				nowPath := []boson.Address{dest, s.addr}
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

	go s.saveRespRouteItem(ctx, respChan{
		neighbor: p.Address,
		pb:       resp,
	})
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
			ct, cancel := context.WithTimeout(ctx, pendingTimeout)
			defer cancel()
			resCh := make(chan []RouteItem, len(forward))
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
				err = fmt.Errorf("route: FindRoute dest %s timeout %.0fs", dest.String(), pendingTimeout.Seconds())
			case routes = <-resCh:
			}
			return
		}
		s.logger.Errorf("route: FindRoute dest %s , neighbor len equal 0", dest.String())
		err = fmt.Errorf("neighbor len equal 0")
	}
	return
}

func (s *Service) GetRoute(ctx context.Context, dest boson.Address) (routes []RouteItem, err error) {
	var has bool
	destKey := common.BytesToHash(dest.Bytes())
	if s.routeTable.TryRLock(dest.ByteString()) {
		defer s.routeTable.RUnlock(dest.ByteString())
		routes, has = s.routeTable.items[destKey]
		if has && len(routes) > 0 {
			return
		}
		err = fmt.Errorf("route not found")
		return
	}
	time.After(time.Millisecond * 50)
	return s.GetRoute(ctx, dest)
}

func (s *Service) burn() {
	ticker := time.NewTicker(burnTickTime)
	for {
		<-ticker.C
		for destKey, items := range s.routeTable.items {
			if s.routeTable.TryLock(destKey.String()) {
				nowItems := make([]RouteItem, 0)
				for _, v := range items {
					if time.Now().Unix()-v.createTime < burnTime.Milliseconds()*1000 {
						v.nextHop = burnNextHop(v.nextHop)
						nowItems = append(nowItems, v)
					}
				}
				if len(nowItems) > 0 {
					s.routeTable.items[destKey] = nowItems
				} else {
					delete(s.routeTable.items, destKey)
				}
			}
		}
	}
}

func (s *Service) pendingClean() {
	ticker := time.NewTicker(pendTickTime)
	for {
		<-ticker.C
		for destKey, item := range s.pendingCalls {
			now := pendingCallResArray{}
			for _, v := range item {
				if time.Since(v.createTime).Seconds() < pendingTimeout.Seconds() {
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

func burnNextHop(old []RouteItem) []RouteItem {
	now := make([]RouteItem, 0)
	for _, v := range old {
		if time.Now().Unix()-v.createTime < burnTime.Milliseconds()*1000 {
			v.nextHop = burnNextHop(v.nextHop)
			now = append(now, v)
		}
	}
	return now
}

func (s *Service) saveRespRouteItem(ctx context.Context, resp respChan) {
	minTTL := maxTTL
	for _, v := range resp.pb.RouteItems {
		if uint8(v.Ttl) < minTTL {
			minTTL = uint8(v.Ttl)
		}
	}
	now := []RouteItem{{
		createTime: time.Now().Unix(),
		ttl:        minTTL + 1,
		neighbor:   resp.neighbor,
		nextHop:    convPbItemListToRouteList(resp.pb.RouteItems),
	}}
	destKey := common.BytesToHash(resp.pb.Dest)
	if s.routeTable.TryLock(destKey.String()) {
		defer s.routeTable.Unlock(destKey.String())
		// save route
		old, ok := s.routeTable.items[destKey]
		if ok {
			now = pathToUpdateRouteList(now, old)
		}
		s.routeTable.items[destKey] = now
	} else {
		s.saveRespRouteItem(ctx, resp)
	}
	// doing resp
	res, has := s.pendingCalls[destKey]
	if has {
		delete(s.pendingCalls, destKey)
		for _, v := range res {
			if !v.src.Equal(s.addr) {
				// forward
				dest := boson.NewAddress(resp.pb.Dest)
				s.doResp(ctx, p2p.Peer{Address: v.src}, dest, now)
			} else if v.resCh != nil {
				// sync return
				v.resCh <- now
			}
		}
	}
}

// the path param only single path and first is dest
func (s *Service) saveReqRouteItem(path [][]byte) {
	pathAddress := make([]boson.Address, len(path))
	for k, v := range path {
		addr := boson.NewAddress(v)
		pathAddress[k] = addr
	}
	for i, addr := range pathAddress {
		now := pathToRouteItem(pathAddress[i:])
		addrKey := common.BytesToHash(addr.Bytes())
		timeout := time.Now().Unix()
		for {
			if s.routeTable.TryLock(addrKey.String()) {
				old, has := s.routeTable.items[addrKey]
				if has {
					// update
					now = pathToUpdateRouteList(now, old)
				}
				s.routeTable.items[addrKey] = now
				s.routeTable.Unlock(addrKey.String())
			}
			time.After(time.Millisecond * 50)
			if time.Now().Unix()-timeout > 10 {
				continue
			}
		}
	}
}

func inNeighbor(addr boson.Address, items []RouteItem) (now []RouteItem, index int, has bool) {
	now = make([]RouteItem, 0)
	for _, v := range items {
		if time.Now().Unix()-v.createTime < burnTime.Milliseconds()*1000 {
			now = append(now, v)
			if v.neighbor.Equal(addr) {
				// only one match
				has = true
				index = len(now) - 1
			}
		}
	}
	return
}

func pathToUpdateRouteList(nowList, oldList []RouteItem) (routes []RouteItem) {
	routesNew := make([]RouteItem, 0)
	for _, now := range nowList {
		tmp := make([]RouteItem, 0)
		for _, old := range oldList {
			if now.neighbor.Equal(old.neighbor) {
				now = pathToUpdateRouteItem(now, old)
			} else {
				tmp = append(tmp, old)
			}
		}
		routesNew = append(routesNew, now)
		oldList = tmp
	}
	if len(oldList) > 0 {
		routesNew = append(routesNew, oldList...)
	}
	return routesNew
}

func pathToUpdateRouteItem(now, old RouteItem) (route RouteItem) {
	if now.neighbor.Equal(old.neighbor) {
		if old.ttl > now.ttl {
			old.ttl = now.ttl
		}
		old.createTime = time.Now().Unix()
		for _, x := range now.nextHop {
			nowNext, index, has := inNeighbor(x.neighbor, old.nextHop)
			if has {
				nowNext[index] = pathToUpdateRouteItem(x, nowNext[index])
				old.nextHop = nowNext
			} else {
				old.nextHop = append(old.nextHop, x)
			}
		}
		route = old
	}
	return
}

// example path [a,b,c,d,e] the first is dest
// return e-d-c-b
func pathToRouteItem(path []boson.Address) (routes []RouteItem) {
	ttl := len(path) - 1
	if ttl < 1 {
		return
	}
	route := RouteItem{
		createTime: time.Now().Unix(),
		ttl:        1,
		neighbor:   path[1],
	}
	for i := 2; i <= ttl; i++ {
		itemNew := RouteItem{
			createTime: time.Now().Unix(),
			ttl:        uint8(ttl),
			neighbor:   path[i],
			nextHop:    []RouteItem{route},
		}
		route = itemNew
	}
	return []RouteItem{route}
}

func (s *Service) getNeighbor() (forward []boson.Address) {
	forward = make([]boson.Address, 0)
	cnt := 0
	err := s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if u >= s.kad.NeighborhoodDepth() {
			// neighbor
			forward = append(forward, address)
			cnt++
			if cnt >= neighborAlpha {
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

func inPath(b []byte, path [][]byte) bool {
	s := string(b)
	for _, v := range path {
		if string(v) == s {
			return true
		}
	}
	return false
}

func convRouteListToPbRouteList(srcList []RouteItem) []*pb.RouteItem {
	out := make([]*pb.RouteItem, len(srcList))
	for k, src := range srcList {
		out[k] = convRouteItemToPbRoute(src)
	}
	return out
}

func convRouteItemToPbRoute(src RouteItem) *pb.RouteItem {
	out := &pb.RouteItem{
		CreateTime: src.createTime,
		Ttl:        uint32(src.ttl),
		Neighbor:   src.neighbor.Bytes(),
		NextHop:    make([]*pb.RouteItem, len(src.nextHop)),
	}
	for k, v := range src.nextHop {
		out.NextHop[k] = convRouteItemToPbRoute(v)
	}
	return out
}

func convPbItemListToRouteList(srcList []*pb.RouteItem) []RouteItem {
	out := make([]RouteItem, 0)
	for _, src := range srcList {
		out = append(out, convPbItemToRouteItem(src))
	}
	return out
}

func convPbItemToRouteItem(src *pb.RouteItem) RouteItem {
	out := RouteItem{
		createTime: src.CreateTime,
		ttl:        uint8(src.Ttl),
		neighbor:   boson.NewAddress(src.Neighbor),
		nextHop:    make([]RouteItem, len(src.NextHop)),
	}
	for k, v := range src.NextHop {
		out.nextHop[k] = convPbItemToRouteItem(v)
	}
	return out
}

func (s *Service) doReq(ctx context.Context, src boson.Address, peer p2p.Peer, dest boson.Address, req *pb.FindRouteReq, ch chan []RouteItem) {
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

func (s *Service) pendingAdd(dest, src boson.Address, ch chan []RouteItem) {
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
		RouteItems: convRouteListToPbRouteList(routes),
	}
	w := protobuf.NewWriter(stream)
	err := w.WriteMsgWithContext(ctx, resp)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("route: doResp write msg: err=%s", err)
	}
}
