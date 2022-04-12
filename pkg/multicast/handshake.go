package multicast

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
)

func (s *Service) Handshake(ctx context.Context, addr boson.Address) (err error) {
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	start := time.Now()
	var stream p2p.Stream
	stream, err = s.getStream(ctx, addr, streamHandshake)
	if err != nil {
		s.logger.Tracef("group: handshake new stream %s %s", addr, err)
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

	s.logger.Tracef("group: send handshake syn")

	err = w.WriteMsgWithContext(ctx, &pb.GIDs{Gid: GIDs})
	if err != nil {
		s.logger.Errorf("multicast Handshake write %s", err)
		return err
	}

	s.logger.Tracef("group: receive handshake ack")

	resp := &pb.GIDs{}
	err = r.ReadMsgWithContext(ctx, resp)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("handshake failed")
		}
		return err
	}

	s.kad.RecordPeerLatency(addr, time.Since(start))

	for _, v := range resp.Gid {
		gid := boson.NewAddress(v)
		s.logger.Tracef("group: exchange group info: %s", gid)
		if s.route.IsNeighbor(addr) {
			s.connectedAddToGroup(gid, addr)
			s.logger.Tracef("group: handshake connected %s with gid %s", addr, gid)
		} else {
			s.keepAddToGroup(gid, addr)
			s.logger.Tracef("group: handshake keep %s with gid %s", addr, gid)
		}
	}
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

	s.logger.Tracef("group: receive handshake syn")

	err = r.ReadMsgWithContext(ctx, resp)
	if err != nil {
		s.logger.Errorf("multicast HandshakeIncoming read %s", err)
		return err
	}

	for _, v := range resp.Gid {
		gid := boson.NewAddress(v)
		s.logger.Tracef("group: exchange group info: %s", gid)
		if s.route.IsNeighbor(peer.Address) {
			s.connectedAddToGroup(gid, peer.Address)
			s.logger.Tracef("HandshakeIncoming connected %s with gid %s", peer.Address, gid)
		} else {
			s.keepAddToGroup(gid, peer.Address)
			s.logger.Tracef("HandshakeIncoming keep %s with gid %s", peer.Address, gid)
		}
	}

	GIDs := s.getGIDsByte()

	s.logger.Tracef("group: send back handshake ack")

	err = w.WriteMsgWithContext(ctx, &pb.GIDs{Gid: GIDs})
	if err != nil {
		s.logger.Errorf("multicast HandshakeIncoming write %s", err)
		return err
	}

	s.logger.Tracef("group: handshake ok")

	return nil
}
