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
	protocolVersion         = "1.0.0"
	streamChunkInfoReqName  = "chunkinforeq"
	streamChunkInfoRespName = "chunkinforesp"
	streamPyramidHashName   = "chunkpyramidhash"
	streamPyramidChunkName  = "chunkpyramidchunk"
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
				Name:    streamPyramidHashName,
				Handler: ci.handlerPyramidHash,
			},
			{
				Name:    streamPyramidChunkName,
				Handler: ci.handlerPyramidChunk,
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

func (ci *ChunkInfo) sendPyramids(ctx context.Context, address boson.Address, streamName string, msg interface{}) (interface{}, boson.Address, error) {

	topCtx := ctx
	if err := ci.route.Connect(ctx, address); err != nil {
		overlays, errs := ci.route.GetTargetNeighbor(ctx, address, totalRouteCount)
		if errs != nil {
			ci.logger.Errorf("[pyramid info] connect: %w", errs)
			return nil, boson.ZeroAddress, errs
		}
		for i, overlay := range overlays {
			ctx := tracing.WithContext(context.Background(), tracing.FromContext(topCtx))
			v, err := ci.sendPyramid(ctx, overlay, streamName, msg)
			if err != nil {
				if i == len(overlays)-1 {
					ci.metrics.PyramidTotalErrors.Inc()
					return nil, boson.ZeroAddress, err
				}
				continue
			}
			return v, overlay, nil
		}
		return nil, boson.ZeroAddress, err
	}
	msg, err := ci.sendPyramid(ctx, address, streamName, msg)
	return msg, address, err
}

func (ci *ChunkInfo) sendPyramid(ctx context.Context, address boson.Address, streamName string, msg interface{}) (interface{}, error) {
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
	switch streamName {
	case streamPyramidHashName:
		req := msg.(pb.ChunkPyramidHashReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			return nil, fmt.Errorf("[pyramid info] write message: %w", err)
		}
		var resp pb.ChunkPyramidHashResp
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			return nil, fmt.Errorf("[pyramid info] read message: %w", err)
		}
		return resp, nil
	case streamPyramidChunkName:
		req := msg.(pb.ChunkPyramidChunkReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			return nil, fmt.Errorf("[pyramid info] write message: %w", err)
		}
		var resp pb.ChunkPyramidChunkResp
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			return nil, fmt.Errorf("[pyramid info] read message: %w", err)
		}
		return resp, nil
	}
	ci.metrics.PyramidRequestCounter.Inc()
	return nil, nil
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

func (ci *ChunkInfo) handlerPyramidHash(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidHashReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[pyramid hash] read pyramid hash message: %w", err)
		return fmt.Errorf("[chunk info] read pyramid hash message: %w", err)
	}
	ci.logger.Tracef("[pyramid hash]  got pyramid req: %q", req)

	target := boson.NewAddress(req.GetTarget())

	var resp pb.ChunkPyramidHashResp
	if target.Equal(ci.addr) || ci.cp.isExists(boson.NewAddress(req.GetRootCid())) {
		hashs, err := ci.onChunkPyramidHashReq(ctx, nil, req)
		if err != nil {
			return err
		}
		resp = pb.ChunkPyramidHashResp{Hash: hashs}
	} else {
		ci.logger.Tracef("[pyramid chunk] got target: %s ", req.Target)
		chunkResp, err := ci.sendPyramid(ctx, target, streamPyramidHashName, req)
		if err != nil {
			return err
		}
		resp = chunkResp.(pb.ChunkPyramidHashResp)
		if err := ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.GetRootCid()), target, resp); err != nil {
			return err
		}
	}

	if err := w.WriteMsgWithContext(ctx, &resp); err != nil {
		return fmt.Errorf("[pyramid hash] write hash message: %w", err)
	}
	return nil
}

func (ci *ChunkInfo) handlerPyramidChunk(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {

	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidChunkReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[pyramid chunk] read pyramid hash message: %w", err)
		return fmt.Errorf("[chunk info] read pyramid hash message: %w", err)
	}
	ci.logger.Tracef("[pyramid chunk]  got pyramid req: %q", req)
	var resp pb.ChunkPyramidChunkResp
	target := boson.NewAddress(req.GetTarget())
	if target.Equal(ci.addr) || ci.cp.isExists(boson.NewAddress(req.GetRootCid())) {
		chunks, err := ci.onChunkPyramidChunkReq(ctx, nil, req)
		if err != nil {
			return err
		}
		resp = pb.ChunkPyramidChunkResp{Chunk: chunks}
	} else {
		ci.logger.Tracef("[pyramid chunk] got target: %s ", req.Target)
		chunkResp, err := ci.sendPyramid(ctx, target, streamPyramidChunkName, req)
		if err != nil {
			return err
		}
		resp = chunkResp.(pb.ChunkPyramidChunkResp)
	}

	if err := w.WriteMsgWithContext(ctx, &resp); err != nil {
		return fmt.Errorf("[pyramid chunk] write hash message: %w", err)
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

func (ci *ChunkInfo) onChunkPyramidHashReq(ctx context.Context, authInfo []byte, req pb.ChunkPyramidHashReq) ([][]byte, error) {
	rootCid := boson.NewAddress(req.RootCid)
	return ci.getChunkPyramidHash(ctx, rootCid)
}

func (ci *ChunkInfo) onChunkPyramidChunkReq(ctx context.Context, authInfo []byte, req pb.ChunkPyramidChunkReq) ([]byte, error) {
	rootCid := boson.NewAddress(req.RootCid)
	return ci.getChunkPyramidChunk(ctx, rootCid, req.Hash)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, resp pb.ChunkPyramidHashResp) error {
	return ci.onFindChunkPyramid(ctx, authInfo, rootCid, overlay, resp.Hash)
}

func (ci *ChunkInfo) onFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, hashs [][]byte) error {
	_, ok := ci.cp.pyramid[rootCid.String()]
	if ok {
		return nil
	}

	var pyramid = make(map[string][]byte)
	for _, hash := range hashs {
		req := pb.ChunkPyramidChunkReq{
			Hash:    hash,
			RootCid: rootCid.Bytes(),
			Target:  overlay.Bytes(),
		}
		v, err := ci.sendPyramid(ctx, overlay, streamPyramidChunkName, req)
		if err != nil {
			return err
		}
		resp := v.(pb.ChunkPyramidChunkResp)
		pyramid[boson.NewAddress(hash).String()] = resp.Chunk
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
	overlays := make([][]byte, 0, len(chunkInfo))
	for over, bv := range chunkInfo {
		overlays = append(overlays, boson.MustParseHexAddress(over).Bytes())
		ci.updateChunkInfo(rootCid, boson.MustParseHexAddress(over), bv)
	}
	ci.syncLk.Lock()
	if msgChan, ok := ci.syncMsg[rootCid.String()]; ok {
		msgChan <- true
	}
	ci.syncLk.Unlock()
	ci.updateQueue(ctx, authInfo, rootCid, overlay, overlays)
}
