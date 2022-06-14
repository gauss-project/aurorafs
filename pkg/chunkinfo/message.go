package chunkinfo

import (
	"context"
	"fmt"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/tracing"
)

const (
	protocolName            = "chunkinfo"
	protocolVersion         = "3.0.0"
	streamChunkInfoReqName  = "chunkinforeq"
	streamChunkInfoRespName = "chunkinforesp"
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
				ci.metrics.DiscoverTotalErrors.Inc()
				if i == len(overlays)-1 {
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
	ci.metrics.DiscoverRequestCounter.Inc()
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
			go stream.FullClose()
		}
	}()
	w, _ := protobuf.NewWriterAndReader(stream)
	switch streamName {
	case streamChunkInfoReqName:
		req := msg.(pb.ChunkInfoReq)
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
	ci.logger.Tracef("got chunk info req")
	return nil
}

func (ci *ChunkInfo) handlerChunkInfoReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			stream.FullClose()
		}
	}()
	var req pb.ChunkInfoReq
	if err = r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[chunk info] read chunk info req message: %w", err)
		return fmt.Errorf("read chunk info req message: %w", err)
	}
	ci.logger.Tracef("[chunk info] got chunk info req.")

	target := boson.NewAddress(req.GetTarget())
	if target.Equal(ci.addr) {
		return ci.onChunkInfoReq(ctx, nil, p.Address, req)
	} else {
		return ci.sendData(ctx, target, streamChunkInfoReqName, req)
	}
}

func (ci *ChunkInfo) handlerChunkInfoResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			stream.FullClose()
		}
	}()
	var resp pb.ChunkInfoResp
	if err = r.ReadMsgWithContext(ctx, &resp); err != nil {
		ci.logger.Errorf("[chunk info] read message: %w", err)
		return fmt.Errorf("[chunk info] read message: %w", err)
	}
	ci.logger.Tracef("[chunk info] got chunk info resp.")
	overlay := boson.NewAddress(resp.GetReq())
	target := boson.NewAddress(resp.GetTarget())

	if overlay.Equal(ci.addr) {
		ci.onChunkInfoResp(context.Background(), nil, target, resp)
		ci.metrics.DiscoverTotalRetrieved.Inc()
	} else {
		return ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
	}
	return
}

func (ci *ChunkInfo) onChunkInfoReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkInfoReq) error {
	rc := boson.NewAddress(req.RootCid)
	info := make(map[string][]byte)
	consumerList, err := ci.chunkStore.GetChunk(chunkstore.SERVICE, rc)
	if err != nil {
		return err
	}
	for i := range consumerList {
		info[consumerList[i].Overlay.String()] = consumerList[i].B
	}
	resp := ci.createChunkInfoResp(rc, info, req.GetTarget(), req.Req)
	return ci.sendData(ctx, overlay, streamChunkInfoRespName, resp)
}

func (ci *ChunkInfo) onChunkInfoResp(ctx context.Context, authInfo []byte, overlay boson.Address, resp pb.ChunkInfoResp) {
	ci.onFindChunkInfo(ctx, authInfo, boson.NewAddress(resp.RootCid), overlay, resp.Presence)
}

func (ci *ChunkInfo) onFindChunkInfo(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, chunkInfo map[string][]byte) {
	ci.timeoutTrigger.removeTimeOutTrigger(rootCid, overlay)
	if chunkInfo == nil {
		chunkInfo = make(map[string][]byte, 1)
	}
	msgChan, ok := ci.syncMsg.Load(rootCid.String())
	if ok {
		msgChan.(chan bool) <- true
	}

	ov := overlay.String()
	if chunkInfo[ov] != nil {
		ci.updateDiscover(ctx, rootCid, overlay, chunkInfo[ov])
	}

	ci.updateQueue(ctx, authInfo, rootCid, overlay, chunkInfo)

	ci.doFindChunkInfo(ctx, authInfo, rootCid)
}

func (ci *ChunkInfo) createChunkInfoReq(rootCid, target, req boson.Address) pb.ChunkInfoReq {
	ciReq := pb.ChunkInfoReq{RootCid: rootCid.Bytes(), Target: target.Bytes(), Req: req.Bytes()}
	return ciReq
}

func (ci *ChunkInfo) createChunkInfoResp(rootCid boson.Address, ctn map[string][]byte, target, req []byte) pb.ChunkInfoResp {
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Target: target, Req: req, Presence: ctn}
}
