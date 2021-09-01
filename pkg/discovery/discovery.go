// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package discovery exposes the discovery driver interface
// which is implemented by discovery protocols.
package discovery

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

type Driver interface {
	BroadcastPeers(ctx context.Context, addressee boson.Address, peers ...boson.Address) error
}
//
//const (
//	protocolName    = "discovery"
//	protocolVersion = "1.0.0"
//	streamName      = "discovery"
//)
//
//type Service struct {
//	streamer        p2p.Streamer
//	addressBook     addressbook.GetPutter
//	addPeersHandler func(...boson.Address) error
//	networkID       uint64
//	logger          logging.Logger
//	kad             *kademlia.Kad
//}
//
//type Config struct {
//	kad *kademlia.Kad
//}
//
//func New(streamer p2p.Streamer, addressBook addressbook.GetPutter, networkID uint64, logger logging.Logger) *Service {
//	return &Service{
//		streamer:    streamer,
//		logger:      logger,
//		addressBook: addressBook,
//		networkID:   networkID,
//	}
//}
//
//func (s *Service) SetConfig(config Config) {
//	s.kad = config.kad
//}
//
//func (s *Service) Protocol() p2p.ProtocolSpec {
//	return p2p.ProtocolSpec{
//		Name:    protocolName,
//		Version: protocolVersion,
//		StreamSpecs: []p2p.StreamSpec{
//			{
//				Name:    streamName,
//				Handler: s.handler,
//			},
//		},
//	}
//}
//
//func (s *Service) handler(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
//	w, r := protobuf.NewWriterAndReader(stream)
//	var req pb.FindNodeReq
//	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
//		_ = stream.Reset()
//		return fmt.Errorf("read findNode handler message: %w", err)
//	}
//
//	defer stream.FullClose()
//
//	var resp *pb.Peers
//	_ = s.kad.EachPeerRev(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
//		if u == uint8(req.Bin) {
//			p, _ := s.addressBook.Get(address)
//			resp.Peers = append(resp.Peers, &pb.AuroraAddress{
//				Underlay:  p.Underlay.Bytes(),
//				Signature: p.Signature,
//				Overlay:   p.Overlay.Bytes(),
//			})
//			if len(resp.Peers) >= int(req.Max) {
//				return true, false, nil
//			}
//			return false, false, nil
//		} else if u > uint8(req.Bin) {
//			return true, false, nil
//		}
//		return false, true, nil
//	})
//
//	err := w.WriteMsgWithContext(ctx, resp)
//	if err != nil {
//		_ = stream.Reset()
//		return fmt.Errorf("write findNode handler message: %w", err)
//	}
//	return nil
//}
//
//func (s Service) findNode(ctx context.Context, target boson.Address) {
//
//}
