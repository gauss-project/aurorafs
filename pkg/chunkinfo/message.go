package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"time"
)

const (
	protocolName            = "chunkinfo"
	protocolVersion         = "2.0.0"
	streamChunkInfoReqName  = "chunkinforeq"
	streamChunkInfoRespName = "chunkinforesp"
	streamPyramidName       = "chunkpyramidhash"
	totalRouteCount         = 3
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
				Name:    streamPyramidName,
				Handler: ci.handlerPyramid,
			},
		},
	}
}

func (ci *ChunkInfo) sendDatas(ctx context.Context, address boson.Address, streamName string, msg interface{}) error {
	topCtx := ctx
	ctx1 := tracing.WithContext(context.Background(), tracing.FromContext(topCtx))
	if err := ci.route.Connect(ctx1, address); err != nil {
		overlays, errs := ci.route.GetTargetNeighbor(ctx, address, totalRouteCount)
		if errs != nil {
			ci.logger.Errorf("[chunk info] connect: %w", errs)
			return errs
		}
		for i, overlay := range overlays {
			ctx := tracing.WithContext(context.Background(), tracing.FromContext(topCtx))
			if err := ci.sendData(ctx, overlay, streamName, msg); err != nil {
				if i == len(overlays)-1 {
					ci.metrics.ChunkInfoTotalErrors.Inc()
					return err
				}
				continue
			}
			return nil
		}
		return err
	}
	return ci.sendData(ctx, address, streamName, msg)
}

func (ci *ChunkInfo) sendData(ctx context.Context, address boson.Address, streamName string, msg interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, TimeOut*time.Second)
	defer cancel()
	if err := ci.route.Connect(ctx, address); err != nil {
		return err
	}

	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		ci.logger.Errorf("[chunk info] new stream: %w", err)
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
	switch streamName {
	case streamChunkInfoReqName:
		req := msg.(pb.ChunkInfoReq)
		ci.logger.Tracef("[chunk info] %s : %s %s", boson.NewAddress(req.GetRootCid()).String(), boson.NewAddress(req.GetTarget()).String(), boson.NewAddress(req.GetReq()).String())
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("[chunk info req] write message: %w", err)
			return err
		}
	case streamChunkInfoRespName:
		req := msg.(pb.ChunkInfoResp)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			ci.logger.Errorf("[chunk info resp] write message: %w", err)
			return err
		}
	}
	ci.metrics.ChunkInfoRequestCounter.Inc()
	ci.logger.Tracef("got chunk info req %q", msg)
	return nil
}

func (ci *ChunkInfo) sendPyramids(ctx context.Context, address boson.Address, streamName string, msg interface{}) (interface{}, error) {

	topCtx := ctx
	if err := ci.route.Connect(ctx, address); err != nil {
		overlays, errs := ci.route.GetTargetNeighbor(ctx, address, totalRouteCount)
		if errs != nil {
			ci.logger.Errorf("[pyramid info] connect: %w", errs)
			return nil, errs
		}
		for i, overlay := range overlays {
			ctx := tracing.WithContext(context.Background(), tracing.FromContext(topCtx))
			v, err := ci.sendPyramid(ctx, overlay, streamName, msg)
			if err != nil {
				if i == len(overlays)-1 {
					ci.metrics.PyramidTotalErrors.Inc()
					return nil, err
				}
				continue
			}
			return v, nil
		}
		return nil, err
	}
	msg, err := ci.sendPyramid(ctx, address, streamName, msg)
	return msg, err
}

func (ci *ChunkInfo) sendPyramid(ctx context.Context, address boson.Address, streamName string, msg interface{}) (interface{}, error) {
	ci.metrics.PyramidRequestCounter.Inc()
	ctx, cancel := context.WithTimeout(ctx, TimeOut*time.Second)
	defer cancel()
	if err := ci.route.Connect(ctx, address); err != nil {
		return nil, err
	}
	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		ci.logger.Errorf("[pyramid info] new stream: %w", err)
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	w, r := protobuf.NewWriterAndReader(stream)

	req := msg.(pb.ChunkPyramidHashReq)
	if err := w.WriteMsgWithContext(ctx, &req); err != nil {
		return nil, fmt.Errorf("[pyramid info] write message: %w", err)
	}
	chunkResps := make([]pb.ChunkPyramidChunkResp, 0)
	for {
		var resp pb.ChunkPyramidChunkResp
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			return nil, fmt.Errorf("[pyramid info] read message: %w", err)
		}
		if resp.Ok {
			return chunkResps, nil
		}
		chunkResps = append(chunkResps, resp)
	}
}

func (ci *ChunkInfo) handlerChunkInfoReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var req pb.ChunkInfoReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[chunk info] read chunk info req message: %w", err)
		return fmt.Errorf("read chunk info req message: %w", err)
	}
	ci.logger.Tracef("[chunk info] got chunk info req: %q", req)

	target := boson.NewAddress(req.GetTarget())
	if target.Equal(ci.addr) {
		return ci.onChunkInfoReq(ctx, nil, p.Address, req)
	} else {
		return ci.sendData(ctx, target, streamChunkInfoReqName, req)
	}
}

func (ci *ChunkInfo) handlerChunkInfoResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	defer stream.FullClose()
	var resp pb.ChunkInfoResp
	if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
		ci.logger.Errorf("[chunk info] read message: %w", err)
		return fmt.Errorf("[chunk info] read message: %w", err)
	}
	ci.logger.Tracef("[chunk info] got chunk info resp: %q", resp)
	overlay := boson.NewAddress(resp.GetReq())
	target := boson.NewAddress(resp.GetTarget())

	if overlay.Equal(ci.addr) {
		ci.onChunkInfoResp(context.Background(), nil, target, resp)
	} else {
		return ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
	}
	return nil
}

func (ci *ChunkInfo) handlerPyramid(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidHashReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[pyramid hash] read pyramid hash message: %w", err)
		return fmt.Errorf("[chunk info] read pyramid hash message: %w", err)
	}
	ci.logger.Tracef("[pyramid hash]  got pyramid req: %q", req)

	target := boson.NewAddress(req.GetTarget())

	resps := make([]pb.ChunkPyramidChunkResp, 0)
	if target.Equal(ci.addr) || ci.cp.isExists(boson.NewAddress(req.GetRootCid())) {
		v, err := ci.onChunkPyramidHashReq(ctx, nil, req)
		if err != nil {
			return err
		}
		for hash, chunk := range v {
			resp := pb.ChunkPyramidChunkResp{Hash: boson.MustParseHexAddress(hash).Bytes(), Chunk: chunk, Ok: false}
			resps = append(resps, resp)
		}
		resp := pb.ChunkPyramidChunkResp{Ok: true}
		resps = append(resps, resp)
	} else {
		ci.logger.Tracef("[pyramid chunk] got target: %s ", req.Target)
		chunkResp, err := ci.sendPyramid(ctx, target, streamPyramidName, req)
		if err != nil {
			return err
		}
		resp := chunkResp.([]pb.ChunkPyramidChunkResp)
		if err := ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.GetRootCid()), resp); err != nil {
			return err
		}
	}

	for _, resp := range resps {
		if err := w.WriteMsgWithContext(ctx, &resp); err != nil {
			return fmt.Errorf("[pyramid hash] write hash message: %w", err)
		}
	}

	return nil
}

// onChunkInfoReq
func (ci *ChunkInfo) onChunkInfoReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkInfoReq) error {
	rc := boson.NewAddress(req.RootCid)
	ctn := ci.ct.getNeighborChunkInfo(rc)
	resp := ci.ct.createChunkInfoResp(rc, ctn, req.GetTarget(), req.Req)
	return ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
}

// onChunkInfoResp
func (ci *ChunkInfo) onChunkInfoResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkInfoResp) {
	ci.onFindChunkInfo(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Presence)
}

func (ci *ChunkInfo) onChunkPyramidHashReq(ctx context.Context, authInfo []byte, req pb.ChunkPyramidHashReq) (map[string][]byte, error) {
	rootCid := boson.NewAddress(req.RootCid)
	return ci.getChunkPyramidHash(ctx, rootCid)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(ctx context.Context, authInfo []byte, rootCid boson.Address, resps []pb.ChunkPyramidChunkResp) error {
	_, ok := ci.cp.pyramid[rootCid.String()]
	if ok {
		return nil
	}

	var pyramid = make(map[string][]byte)
	for _, resp := range resps {
		pyramid[boson.NewAddress(resp.Hash).String()] = resp.Chunk
	}

	v, err := ci.traversal.CheckTrieData(ctx, rootCid, pyramid)
	if err != nil {
		ci.logger.Errorf("chunk pyramid: check pyramid error")
		return err
	}
	ci.updateChunkPyramid(rootCid, v, pyramid)
	ci.ct.initNeighborChunkInfo(rootCid)
	return nil
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, chunkInfo map[string][]byte) {
	ci.tt.removeTimeOutTrigger(rootCid, overlay)
	if chunkInfo == nil {
		chunkInfo = make(map[string][]byte, 1)
	}
	ci.syncLk.Lock()
	if msgChan, ok := ci.syncMsg[rootCid.String()]; ok {
		msgChan <- true
	}
	ci.syncLk.Unlock()
	ci.updateQueue(ctx, authInfo, rootCid, overlay, chunkInfo)
}
