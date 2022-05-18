package multicast

import (
	"context"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
)

func (s *Service) Handshake(ctx context.Context, addr boson.Address) (err error) {
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	start := time.Now()
	var stream p2p.Stream
	stream, err = s.getStream(ctx, addr, streamHandshake)
	if err != nil {
		s.logger.Errorf("multicast Handshake to %s %s", addr, err)
		return
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	w, r := protobuf.NewWriterAndReader(stream)

	GIDs := s.getGIDsByte()

	err = w.WriteMsgWithContext(ctx, &pb.GIDs{Gid: GIDs})
	if err != nil {
		s.logger.Errorf("multicast Handshake write %s", err)
		return err
	}

	resp := &pb.GIDs{}
	err = r.ReadMsgWithContext(ctx, resp)
	if err != nil {
		s.logger.Errorf("multicast Handshake read %s", err)
		return err
	}

	s.kad.RecordPeerLatency(addr, time.Since(start))

	s.updatePeerGroupsJoin(addr, ConvertGIDs(resp.Gid))

	return nil
}

func (s *Service) HandshakeIncoming(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	w, r := protobuf.NewWriterAndReader(stream)

	resp := &pb.GIDs{}

	err = r.ReadMsgWithContext(ctx, resp)
	if err != nil {
		s.logger.Errorf("multicast HandshakeIncoming read %s", err)
		return err
	}

	GIDs := s.getGIDsByte()

	err = w.WriteMsgWithContext(ctx, &pb.GIDs{Gid: GIDs})
	if err != nil {
		s.logger.Errorf("multicast HandshakeIncoming write %s", err)
		return err
	}

	s.updatePeerGroupsJoin(peer.Address, ConvertGIDs(resp.Gid))

	return nil
}

func (s *Service) getGIDsByte() [][]byte {
	GIDs := make([][]byte, 0)
	if s.nodeMode.IsBootNode() || !s.nodeMode.IsFull() {
		return GIDs
	}
	list := s.getGroupAll()
	for _, g := range list {
		if g.option.GType == model.GTypeJoin {
			GIDs = append(GIDs, g.gid.Bytes())
		}
	}
	return GIDs
}

func (s *Service) HandshakeAll() {
	start := time.Now()
	s.logger.Debugf("multicast HandshakeAll start")
	wg := sync.WaitGroup{}
	_ = s.kad.EachPeer(func(addr boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Handshake(context.Background(), addr)
		}()
		return false, false, nil
	}, topology.Filter{Reachable: false})
	wg.Wait()
	s.logger.Debugf("multicast HandshakeAll took %s", time.Since(start))
}

func (s *Service) HandshakeAllKept(gs []*Group, connected bool) {
	start := time.Now()
	s.logger.Debugf("multicast HandshakeAllKept start")
	skipMap := make(map[string]struct{})
	wg := sync.WaitGroup{}
	for _, g := range gs {
		if connected {
			for _, v := range g.connectedPeers.BinPeers(0) {
				if _, ok := skipMap[v.ByteString()]; ok {
					continue
				}
				skipMap[v.ByteString()] = struct{}{}
				wg.Add(1)
				go func(addr boson.Address) {
					defer wg.Done()
					_ = s.Handshake(context.Background(), addr)
				}(v)
			}
		}
		for _, v := range g.keepPeers.BinPeers(0) {
			if _, ok := skipMap[v.ByteString()]; ok {
				continue
			}
			skipMap[v.ByteString()] = struct{}{}
			wg.Add(1)
			go func(addr boson.Address) {
				defer wg.Done()
				_ = s.Handshake(context.Background(), addr)
			}(v)
		}
		for _, v := range g.knownPeers.BinPeers(0) {
			if _, ok := skipMap[v.ByteString()]; ok {
				continue
			}
			skipMap[v.ByteString()] = struct{}{}
			wg.Add(1)
			go func(addr boson.Address) {
				defer wg.Done()
				_ = s.Handshake(context.Background(), addr)
			}(v)
		}
		g.pruneKnown()
	}
	wg.Wait()
	s.logger.Debugf("multicast HandshakeAllKept took %s", time.Since(start))
}

func (s *Service) HandshakeAllPeers(ps *pslice.PSlice) {
	start := time.Now()
	s.logger.Debugf("multicast HandshakeAllKept start")
	skipMap := make(map[string]struct{})
	wg := sync.WaitGroup{}
	for _, v := range ps.BinPeers(0) {
		if _, ok := skipMap[v.ByteString()]; ok {
			continue
		}
		skipMap[v.ByteString()] = struct{}{}
		wg.Add(1)
		go func(addr boson.Address) {
			defer wg.Done()
			_ = s.Handshake(context.Background(), addr)
		}(v)
	}
	wg.Wait()
	s.logger.Debugf("multicast HandshakeAllKept took %s", time.Since(start))
}
