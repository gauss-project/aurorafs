package multicast

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
	"github.com/gogf/gf/v2/util/gconv"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
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
		s.groups.Range(func(_, value interface{}) bool {
			v := value.(*Group)
			doFunc(&wg, v)
			return true
		})
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
	skip := make(map[string]int)

	select {
	case <-s.close:
		return
	default:
	}

	t := time.Now()
	s.logger.Tracef("doFindGroup start %s", group.gid)
	defer s.logger.Tracef("doFindGroup done %s took %v", group.gid, time.Since(t))

	switch {
	case group.connectedPeers.Length() > 0:
		s.logger.Tracef("doFindGroup connectedPeers %s got %d nodes", group.gid, group.connectedPeers.Length())
		_ = group.connectedPeers.EachBinRev(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			lim := limit()
			if lim <= 0 {
				return true, false, nil
			}
			ctx := context.Background()
			finds, err := s.getGroupNode(ctx, address, &pb.FindGroupReq{
				Gid:   group.gid.Bytes(),
				Limit: int32(lim),
				Ttl:   0,
				Paths: s.getSkipInGroupPeers(group.gid),
			})
			if err != nil || len(finds) == 0 {
				return false, false, nil
			}
			s.logger.Tracef("doFindGroup got %d node in group %s from connected %s", len(finds), group.gid, address)
			s.keepAddToGroup(group.gid, finds...)
			return false, false, nil
		})
		if limit() <= 0 {
			return
		}
		fallthrough
	case group.keepPeers.Length() > 0:
		lookup := func(list []boson.Address) {
			for _, v := range list {
				lim := limit()
				if lim <= 0 {
					break
				}
				skip[v.String()] = 1
				finds, err := s.getGroupNode(context.Background(), v, &pb.FindGroupReq{
					Gid:   group.gid.Bytes(),
					Limit: int32(lim),
					Ttl:   0,
					Paths: s.getSkipInGroupPeers(group.gid),
				})
				if err != nil || len(finds) == 0 {
					continue
				}
				s.logger.Tracef("doFindGroup got %d node in group %s from keep %s", len(finds), group.gid, v)
				s.keepAddToGroup(group.gid, finds...)
			}
		}
		var list []boson.Address
		getList := func() {
			_ = group.keepPeers.EachBinRev(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
				// needs to skip without the first round
				if _, ok := skip[address.String()]; !ok {
					skip[address.String()] = 1
					list = append(list, address)
				}
				return false, false, nil
			})
		}

		getList()
	LOOP:
		s.logger.Tracef("doFindGroup keepPeers %s got %d nodes", group.gid, group.keepPeers.Length())
		lookup(list)

		if limit() <= 0 {
			return
		}
		list = []boson.Address{}
		getList()
		if len(list) > 0 {
			goto LOOP
		}
		fallthrough
	case group.knownPeers.Length() > 0:
		s.logger.Tracef("doFindGroup knownPeers %s got %d nodes", group.gid, group.knownPeers.Length())
		var knowWg sync.WaitGroup
		_ = group.knownPeers.EachBinRev(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			lim := limit()
			if lim <= 0 {
				return true, false, nil
			}
			if _, ok := skip[address.String()]; ok {
				group.knownPeers.Remove(address)
				return false, false, nil
			}
			skip[address.String()] = 1
			knowWg.Add(1)
			go func() {
				defer knowWg.Done()
				_ = s.Handshake(context.Background(), address)
			}()
			return false, false, nil
		})
		knowWg.Wait()
		if limit() <= 0 {
			return
		}
		fallthrough
	default:
		// skipPeers := make([]boson.Address, 0)
		// for addr := range skip {
		// 	skipPeers = append(skipPeers, boson.MustParseHexAddress(addr))
		// }
		// nodes := s.getForwardNodes(group.gid, skipPeers...)
		//
		// s.logger.Tracef("doFindGroup default %s got %d nodes", group.gid, len(nodes))
		//
		// for _, addr := range nodes {
		// 	lim := limit()
		// 	if lim <= 0 {
		// 		return
		// 	}
		// 	finds, err := s.getGroupNode(context.Background(), addr, &pb.FindGroupReq{
		// 		Gid:   group.gid.Bytes(),
		// 		Limit: int32(lim),
		// 		Ttl:   0,
		// 		Paths: s.getSkipInGroupPeers(group.gid),
		// 	})
		// 	if err != nil || len(finds) == 0 {
		// 		continue
		// 	}
		// 	s.logger.Tracef("doFindGroup got %d node in group %s from forward %s", len(finds), group.gid, addr)
		// 	s.keepAddToGroup(group.gid, finds...)
		// }
	}

}

func (s *Service) getForwardNodes(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	nodes = s.getForwardNodesKnownConnected(gid, skip...)
	if len(nodes) > 0 {
		s.logger.Tracef("multicast forward get known connected %d", len(nodes))
		return nodes
	}
	nodes = s.getForwardNodesSelfGroup(gid, skip...)
	if len(nodes) > 0 {
		s.logger.Tracef("multicast forward get self group %d", len(nodes))
		return nodes
	}
	// nodes = s.getForwardNodesDefault(gid, skip...)
	// if len(nodes) > 0 {
	// 	s.logger.Tracef("multicast forward get default %d", len(nodes))
	// }
	return
}

func (s *Service) getCloserKnownGID(gid boson.Address) (p *pslice.PSlice) {
	closer := boson.ZeroAddress
	s.connectedPeers.Range(func(key, value interface{}) bool {
		v := boson.MustParseHexAddress(gconv.String(key))
		conn := value.(*pslice.PSlice)
		_, ok := s.groups.Load(key)
		if !ok && conn.Length() > 0 {
			if closer.IsZero() {
				closer = v
				p = conn
				return true
			}
			if yes, _ := v.Closer(gid, closer); yes {
				closer = v
				p = conn
			}
		}
		return true
	})
	return
}

// Get forwarding nodes step 1
func (s *Service) getForwardNodesKnownConnected(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	conn := s.getCloserKnownGID(gid)
	if conn != nil {
		_ = conn.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				nodes = append(nodes, address)
			}
			return false, false, nil
		})
		if len(nodes) > forwardLimit {
			nodes = RandomPeersLimit(nodes, forwardLimit)
		}
	}
	return
}

func (s *Service) getCloserGroup(gid boson.Address) (g *Group) {
	closer := boson.ZeroAddress
	s.groups.Range(func(_, value interface{}) bool {
		v := value.(*Group)
		if v.connectedPeers.Length() == 0 && v.keepPeers.Length() == 0 {
			return true
		}
		if closer.IsZero() {
			closer = v.gid
			g = v
			return true
		}
		if yes, _ := v.gid.Closer(gid, closer); yes {
			closer = v.gid
			g = v
		}
		return true
	})
	return g
}

// Get forwarding nodes step 2
func (s *Service) getForwardNodesSelfGroup(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	g := s.getCloserGroup(gid)
	if g != nil {
		_ = g.connectedPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				nodes = append(nodes, address)
			}
			return false, false, nil
		})
		total := len(nodes)
		if total > forwardLimit {
			nodes = RandomPeersLimit(nodes, forwardLimit)
			return
		}
		var keep []boson.Address
		_ = g.keepPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				keep = append(keep, address)
			}
			return false, false, nil
		})
		if len(keep) > forwardLimit-total {
			keep = RandomPeersLimit(keep, forwardLimit-total)
		}
		nodes = append(nodes, keep...)
	}
	return
}

// Get forwarding nodes step 3
func (s *Service) getForwardNodesDefault(gid boson.Address, skip ...boson.Address) (nodes []boson.Address) {
	depth := s.kad.NeighborhoodDepth()
	po := boson.Proximity(s.self.Bytes(), gid.Bytes())

	if po < depth {
		_ = s.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if po != u {
				return false, true, nil
			}
			if !address.MemberOf(skip) {
				nodes = append(nodes, address)
			}
			return false, false, nil
		}, topology.Filter{Reachable: false})
	} else {
		_ = s.kad.EachNeighbor(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				nodes = append(nodes, address)
			}
			return false, false, nil
		})
	}
	if len(nodes) > forwardLimit {
		nodes = RandomPeersLimit(nodes, forwardLimit)
	}
	return
}

func (s *Service) getGroupNode(ctx context.Context, target boson.Address, req *pb.FindGroupReq) (out []boson.Address, err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, target, streamFindGroup)
	if err != nil {
		s.logger.Errorf("getGroupNode new stream %s", err)
		return
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
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
	if errors.Is(err, io.EOF) {
		return
	}
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

	// connected
	value, ok := s.connectedPeers.Load(gid.String())
	if ok {
		conn := value.(*pslice.PSlice)
		if conn.Length() > 0 {
			_ = conn.EachBin(peerFunc)
			if len(resp.Addresses) >= int(req.Limit) {
				return returnResp()
			}
		}
	}

	// keep
	g, ok := s.groups.Load(gid.String())
	if ok {
		v := g.(*Group)
		_ = v.keepPeers.EachBin(peerFunc)
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
	g, ok := s.groups.Load(gid.String())
	if ok {
		v := g.(*Group)
		_ = v.connectedPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			skip = append(skip, address.Bytes())
			return false, false, err
		})
		_ = v.keepPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			skip = append(skip, address.Bytes())
			return false, false, err
		})
	}
	return skip
}
