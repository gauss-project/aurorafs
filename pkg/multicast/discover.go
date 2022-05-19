package multicast

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/multicast/model"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
)

const (
	maxKnownPeers    = 20
	maxTTL           = 10
	forwardLimit     = 2
	discoverInterval = time.Second * 30
)

func (s *Service) StartDiscover() {
	s.discover(nil)
	go func() {
		ticker := time.NewTicker(discoverInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.close:
				return
			case <-ticker.C:
				s.discover(nil)
				ticker.Reset(discoverInterval)
			}
		}
	}()
}

func (s *Service) discover(g *Group) {
	gname := func() string {
		if g != nil {
			return g.option.Name + "/" + g.gid.String()
		} else {
			return "all"
		}
	}
	s.logger.Tracef("multicast discover %s start", gname())
	t := time.Now()
	defer s.logger.Tracef("multicast discover %s done took %s", gname(), time.Since(t))

	doFunc := func(wg *sync.WaitGroup, v *Group) {
		if v.connectedPeers.Length() < v.option.KeepConnectedPeers {
			_ = v.keepPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
				_ = s.route.Connect(context.Background(), address)
				if v.connectedPeers.Length() >= v.option.KeepConnectedPeers {
					return true, false, err
				}
				return false, false, err
			})
		}
		if v.keepPeers.Length() < v.option.KeepPingPeers || v.connectedPeers.Length() < v.option.KeepConnectedPeers {
			// do find
			wg.Add(1)
			go s.doFindGroup(wg, v)
		}
	}
	var wg sync.WaitGroup
	if g != nil {
		doFunc(&wg, g)
	} else {
		for _, v := range s.getGroupAll() {
			doFunc(&wg, v)
		}
	}
	wg.Wait()
}

func (s *Service) doFindGroup(wg *sync.WaitGroup, group *Group) {
	defer wg.Done()
	limit := func() int {
		var a, b, at, bt int
		at = group.connectedPeers.Length()
		bt = group.keepPeers.Length()
		if group.option.KeepConnectedPeers > at {
			a = group.option.KeepConnectedPeers - at
		}
		if group.option.KeepPingPeers > bt {
			b = group.option.KeepPingPeers - bt
		}
		return a + b
	}
	if limit() <= 0 {
		return
	}
	skip := make(map[string]struct{})
	getNodes := func(list []boson.Address, tag string) {
		for _, v := range list {
			lim := limit()
			if lim <= 0 {
				break
			}
			skip[v.String()] = struct{}{}
			finds, err := s.getGroupNode(context.Background(), v, &pb.FindGroupReq{
				Gid:   group.gid.Bytes(),
				Limit: int32(lim),
				Ttl:   0,
				Paths: s.getSkipInGroupPeers(group.gid),
			})
			if err != nil || len(finds) == 0 {
				continue
			}
			s.logger.Tracef("doFindGroup got %d node in group %s from %s %s", len(finds), group.gid, tag, v)
			for _, addr := range finds {
				group.add(addr, false)
			}
		}
	}

	select {
	case <-s.close:
		return
	default:
	}

	t := time.Now()
	s.logger.Infof("doFindGroup start %s", group.gid)
	defer s.logger.Infof("doFindGroup done %s took %v", group.gid, time.Since(t))

	if group.connectedPeers.Length() > 0 {
		s.logger.Tracef("doFindGroup connectedPeers %s got %d nodes", group.gid, group.connectedPeers.Length())
		getNodes(group.connectedPeers.BinPeers(0), "connected")
		if limit() <= 0 {
			return
		}
	}
	if group.keepPeers.Length() > 0 {
		s.logger.Tracef("doFindGroup keepPeers %s got %d nodes", group.gid, group.keepPeers.Length())
		getNodes(group.keepPeers.BinPeers(0), "kept")
		if limit() <= 0 {
			return
		}
	}
	if group.knownPeers.Length() > 0 {
		s.logger.Tracef("doFindGroup knownPeers %s got %d nodes", group.gid, group.knownPeers.Length())
		s.HandshakeAllPeers(group.knownPeers)
		group.pruneKnown()
	}
}

func (s *Service) getForwardNodes(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	nodes = s.getForwardNodesKnownConnected(gid, skip...)
	if len(nodes) > 0 {
		s.logger.Tracef("multicast forward get known group %d", len(nodes))
		return nodes
	}
	nodes = s.getForwardNodesSelfGroup(gid, skip...)
	if len(nodes) > 0 {
		s.logger.Tracef("multicast forward get self group %d", len(nodes))
		return nodes
	}
	return
}

func (s *Service) getCloserKnownGID(gid boson.Address) (connected, kept *pslice.PSlice) {
	closer := boson.ZeroAddress
	all := s.getGroupAll()
	for _, g := range all {
		if g.option.GType != model.GTypeJoin && (g.connectedPeers.Length() > 0 || g.keepPeers.Length() > 0) {
			if closer.IsZero() {
				closer = g.gid
				connected = g.connectedPeers
				kept = g.keepPeers
				continue
			}
			if yes, _ := g.gid.Closer(gid, closer); yes {
				closer = g.gid
				connected = g.connectedPeers
				kept = g.keepPeers
			}
		}
	}
	return
}

// Get forwarding nodes step 1
func (s *Service) getForwardNodesKnownConnected(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	conn, kept := s.getCloserKnownGID(gid)
	return s.getForward(conn, kept, skip...)
}

func (s *Service) getCloserSelfGID(gid boson.Address) (connected, kept *pslice.PSlice) {
	closer := boson.ZeroAddress
	all := s.getGroupAll()
	for _, g := range all {
		if g.option.GType == model.GTypeJoin && (g.connectedPeers.Length() > 0 || g.keepPeers.Length() > 0) {
			if closer.IsZero() {
				closer = g.gid
				connected = g.connectedPeers
				kept = g.keepPeers
				continue
			}
			if yes, _ := g.gid.Closer(gid, closer); yes {
				closer = g.gid
				connected = g.connectedPeers
				kept = g.keepPeers
			}
		}
	}
	return
}

// Get forwarding nodes step 2
func (s *Service) getForwardNodesSelfGroup(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	conn, kept := s.getCloserSelfGID(gid)
	return s.getForward(conn, kept, skip...)
}

func (s *Service) getForward(conn, kept *pslice.PSlice, skip ...boson.Address) (nodes []boson.Address) {
	if conn != nil && conn.Length() > 0 {
		for _, v := range conn.BinPeers(0) {
			if !v.MemberOf(skip) {
				nodes = append(nodes, v)
			}
		}
		if len(nodes) > forwardLimit {
			nodes = RandomPeersLimit(nodes, forwardLimit)
			return
		}
	}
	if kept != nil && kept.Length() > 0 {
		var list []boson.Address
		for _, v := range conn.BinPeers(0) {
			if !v.MemberOf(skip) {
				list = append(list, v)
			}
		}
		k := forwardLimit - len(nodes)
		if len(list) > k {
			list = RandomPeersLimit(list, forwardLimit-len(nodes))
		}
		nodes = append(nodes, list...)
	}
	return
}

func (s *Service) getGroupNode(ctx context.Context, target boson.Address, req *pb.FindGroupReq) (out []boson.Address, err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, target, streamFindGroup)
	if err != nil {
		s.logger.Errorf("getGroupNode %s", err)
		return
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.Close()
		}
	}()
	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx, req)
	if err != nil {
		s.logger.Errorf("getGroupNode write %s", err)
		return
	}
	resp := &pb.FindGroupResp{}
	err = r.ReadMsgWithContext(ctx, resp)
	if err != nil {
		s.logger.Errorf("getGroupNode read %s", err)
		return
	}
	for _, v := range resp.Addresses {
		out = append(out, boson.NewAddress(v))
	}
	return
}

func (s *Service) onFindGroup(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)
	req := &pb.FindGroupReq{}
	err = r.ReadMsgWithContext(ctx, req)
	if err != nil {
		s.logger.Errorf("onFindGroup read %s", err)
		return err
	}

	s.logger.Tracef("onFindGroup receive from: %s", peer.Address)

	req.Ttl++
	req.Paths = append(req.Paths, s.self.Bytes())
	skip := make([]boson.Address, len(req.Paths))
	for _, v := range req.Paths {
		skip = append(skip, boson.NewAddress(v))
	}
	resp := &pb.FindGroupResp{}

	peerFunc := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if address.MemberOf(skip) {
			return false, false, err
		}
		resp.Addresses = append(resp.Addresses, address.Bytes())
		if len(resp.Addresses) >= int(req.Limit) {
			return true, false, err
		}
		return false, false, err
	}

	returnResp := func() error {
		err = w.WriteMsgWithContext(ctx, resp)
		if err != nil {
			s.logger.Errorf("onFindGroup write", err)
			return err
		}
		return nil
	}

	gid := boson.NewAddress(req.Gid)
	g := s.getGroup(gid)

	if g != nil {
		// connected
		if g.connectedPeers.Length() > 0 {
			_ = g.connectedPeers.EachBin(peerFunc)
			if len(resp.Addresses) >= int(req.Limit) {
				return returnResp()
			}
		}

		// keep
		_ = g.keepPeers.EachBin(peerFunc)
		if len(resp.Addresses) > 0 {
			return returnResp()
		}
	}

	if req.Ttl >= maxTTL {
		return fmt.Errorf("onFindGroup outstrip max ttl %d", maxTTL)
	}

	// forward
	nodes := s.getForwardNodes(gid, skip...)

	var finds []boson.Address
	for _, addr := range nodes {
		f1, err := s.getGroupNode(ctx, addr, req)
		if err != nil || len(f1) == 0 {
			continue
		}
		s.logger.Tracef("onFindGroup got %d node in group %s from %s", len(f1), gid, addr)
		finds = append(finds, f1...)
		if len(finds) >= int(req.Limit) {
			break
		}
	}
	if len(finds) == 0 {
		s.logger.Tracef("onFindGroup got 0 node in group %s", gid)
		return nil
	}
	for _, v := range finds {
		resp.Addresses = append(resp.Addresses, v.Bytes())
	}
	return returnResp()
}

func (s *Service) getSkipInGroupPeers(gid boson.Address) (skip [][]byte) {
	skip = append(skip, s.self.Bytes())
	g := s.getGroup(gid)
	if g != nil {
		for _, v := range g.connectedPeers.BinPeers(0) {
			skip = append(skip, v.Bytes())
		}
		for _, v := range g.keepPeers.BinPeers(0) {
			skip = append(skip, v.Bytes())
		}
	}
	return skip
}
