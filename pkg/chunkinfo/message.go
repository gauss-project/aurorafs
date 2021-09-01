package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"time"
)

const (
	protocolName              = "chunkinfo"
	protocolVersion           = "1.0.0"
	streamChunkInfoReqName    = "chunkinforeq"
	streamChunkInfoRespName   = "chunkinforesp"
	streamPyramidReqName      = "chunkpyramidreq"
	streamPyramidRespName     = "chunkpyramidresp"
	streamPyramidHashRespName = "chunkpyramidhashresp"
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
			// todo 新增金字塔hash-》chunk数据获取
		},
	}
}

func (ci *ChunkInfo) sendData(ctx context.Context, address boson.Address, streamName string, msg interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, TimeOut*time.Second)
	defer cancel()
	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		ci.logger.Errorf("new stream: %w", err)
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	w, _ := protobuf.NewWriterAndReader(stream)
	// todo 新增金字塔hash-》chunk数据获取
	switch streamName {
	case streamChunkInfoReqName:
		req := msg.(pb.ChunkInfoReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
			return err
		}
	case streamChunkInfoRespName:
		req := msg.(pb.ChunkInfoResp)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
			return err
		}
	case streamPyramidReqName:
		req := msg.(pb.ChunkPyramidReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
			return err
		}
	case streamPyramidRespName:
		req := msg.(pb.ChunkPyramidResp)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("write message: %w", err)
			return err
		}
	}

	ci.logger.Tracef("got chunk info req %q", msg)
	return nil
}

func (ci *ChunkInfo) handlerChunkInfoReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var req pb.ChunkInfoReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("read chunk info req message: %w", err)
		return fmt.Errorf("read chunk info req message: %w", err)
	}
	ci.logger.Tracef("got chunk info req: %q", req)
	return ci.onChunkInfoReq(ctx, nil, p.Address, req)
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
	return ci.onChunkPyramidReq(ctx, nil, p.Address, req)
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
func (ci *ChunkInfo) onChunkInfoReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkInfoReq) error {
	// todo
	rc := boson.NewAddress(req.RootCid)
	ctn := ci.ct.getNeighborChunkInfo(rc)
	resp := ci.ct.createChunkInfoResp(rc, ctn)
	return ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
}

// onChunkInfoResp
func (ci *ChunkInfo) onChunkInfoResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkInfoResp) {
	ci.onFindChunkInfo(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Presence)
}

// onChunkPyramidReq
func (ci *ChunkInfo) onChunkPyramidReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkPyramidReq) error {
	// todo 请求金字塔逻辑修改
	rootCid := boson.NewAddress(req.RootCid)
	cp, err := ci.getChunkPyramid(ctx, rootCid)
	if err != nil {
		return err
	}
	resp := ci.ct.createChunkPyramidResp(rootCid, cp)
	return ci.sendData(ctx, overlay, streamPyramidRespName, resp)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkPyramidResp) {
	ci.onFindChunkPyramid(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Pyramid, resp.Ctn)
}

// onFindChunkPyramid
func (ci *ChunkInfo) onFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, pyramid map[string][]byte, cn map[string]*pb.Overlays) {
	// todo 获取金字塔结构hash 逻辑修改
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
		ci.ct.initNeighborChunkInfo(rootCid)
	}
	// todo 调用开始节点发现
	//ci.onFindChunkInfo(ctx, authInfo, rootCid, overlay, cn)
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, chunkInfo map[string][]byte) {
	// todo resp 请求体改变
	ci.tt.removeTimeOutTrigger(rootCid, overlay)
	overlays := make([][]byte, 0)
	for overlay, bv := range chunkInfo {
		//  validate rootCid:cid
		//c := boson.MustParseHexAddress(cid)
		//if ci.cp.checkPyramid(rootCid, c) {
		//	overlays = append(overlays, n.V...)
		//	ci.cd.updateChunkInfos(rootCid, c, n.V)
		//}
	}
	ci.updateQueue(ctx, authInfo, rootCid, overlay, overlays)
}
