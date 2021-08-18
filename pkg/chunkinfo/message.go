package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
)

const (
	protocolName            = "chunkinfo"
	protocolVersion         = "1.0.0"
	streamChunkInfoReqName  = "chunkinfo/req"
	streamChunkInfoRespName = "chunkinfo/resp"
	streamPyramidReqName    = "chunkpyramid/req"
	streamPyramidRespName   = "chunkpyramid/resp"
)

func (ci *ChunkInfo) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamChunkInfoReqName,
				Handler: ci.handlerChunkInfoReq,
			}, {
				Name:    streamChunkInfoRespName,
				Handler: ci.handlerChunkInfoResp,
			},
			{
				Name:    streamPyramidReqName,
				Handler: ci.handlerPyramidReq,
			}, {
				Name:    streamPyramidRespName,
				Handler: ci.handlerPyramidResp,
			},
		},
	}
}

func (ci *ChunkInfo) sendData(ctx context.Context, address boson.Address, streamName string, msg interface{}) {

	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		ci.logger.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	w, _ := protobuf.NewWriterAndReader(stream)
	switch streamName {
	case streamChunkInfoReqName:
		req := msg.(pb.ChunkInfoReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
		}
	case streamChunkInfoRespName:
		req := msg.(pb.ChunkInfoResp)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
		}
	case streamPyramidReqName:
		req := msg.(pb.ChunkPyramidReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
		}
	case streamPyramidRespName:
		req := msg.(pb.ChunkPyramidResp)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
		}
	}

	ci.logger.Tracef("got chunk info req %q", msg)
}

func (ci *ChunkInfo) handlerChunkInfoReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	_, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkInfoReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("read chunk info req message: %w", err)
		return fmt.Errorf("read chunk info req message: %w", err)
	}
	ci.logger.Tracef("got chunk info req: %q", req)
	ci.onChunkInfoReq(ctx, nil, p.Address, req)
	return nil
}

func (ci *ChunkInfo) handlerChunkInfoResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var resp pb.ChunkInfoResp
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		ci.logger.Errorf("read message: %w", err)
		return fmt.Errorf("read message: %w", err)
	}
	ci.logger.Tracef("got chunk info resp: %q", resp)
	ci.onChunkInfoResp(ctx, nil, p.Address, resp)
	return nil
}

func (ci *ChunkInfo) handlerPyramidReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("read message: %w", err)
		return fmt.Errorf("read message: %w", err)
	}
	ci.logger.Tracef("got pyramid req: %q", req)
	ci.onChunkPyramidReq(ctx, nil, p.Address, req)
	return nil
}

func (ci *ChunkInfo) handlerPyramidResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var resp pb.ChunkPyramidResp
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		ci.logger.Errorf("read pyramid message: %w", err)
		return fmt.Errorf("read pyramid message: %w", err)
	}
	ci.logger.Tracef("got pyramid resp: %q", resp)
	ci.onChunkPyramidResp(ctx, nil, p.Address, resp)
	return nil
}

// onChunkInfoReq
func (ci *ChunkInfo) onChunkInfoReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkInfoReq) {
	rc := boson.NewAddress(req.RootCid)
	ctn := ci.ct.getNeighborChunkInfo(rc)
	resp := ci.ct.createChunkInfoResp(rc, ctn)
	ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
}

// onChunkInfoResp
func (ci *ChunkInfo) onChunkInfoResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkInfoResp) {
	ci.onFindChunkInfo(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Presence)
}

// onChunkPyramidReq
func (ci *ChunkInfo) onChunkPyramidReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkPyramidReq) {
	rootCid := boson.NewAddress(req.RootCid)
	cp, _ := ci.getChunkPyramid(ctx, rootCid)
	nci := ci.ct.getNeighborChunkInfo(rootCid)
	resp := ci.ct.createChunkPyramidResp(rootCid, cp, nci)
	ci.sendData(ctx, overlay, streamPyramidRespName, resp)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkPyramidResp) {
	ci.onFindChunkPyramid(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Pyramid, resp.Ctn)
}

// onFindChunkPyramid
func (ci *ChunkInfo) onFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, pyramid map[string][]byte, cn map[string]*pb.Overlays) {
	ci.tt.removeTimeOutTrigger(rootCid, overlay)
	_, ok := ci.cp.pyramid[rootCid.String()]
	if !ok {
		// validate pyramid
		v, err := ci.traversal.CheckTrieData(ctx, rootCid, pyramid)
		if err != nil {
			ci.logger.Errorf("chunk pyramid: check pyramid error")
			return
		}
		ci.cp.updateChunkPyramid(rootCid, v)
	}
	ci.onFindChunkInfo(ctx, authInfo, rootCid, overlay, cn)
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, chunkInfo map[string]*pb.Overlays) {
	ci.tt.removeTimeOutTrigger(rootCid, overlay)
	overlays := make([][]byte, 0)
	for cid, n := range chunkInfo {
		//  validate rootCid:cid
		if ci.cp.checkPyramid(rootCid, boson.MustParseHexAddress(cid)) {
			overlays = append(overlays, n.V...)
		}
	}
	ci.cd.updateChunkInfos(rootCid, chunkInfo)
	ci.updateQueue(ctx, authInfo, rootCid, overlay, overlays)
}
