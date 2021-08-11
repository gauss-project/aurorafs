package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"io"
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

func (ci *ChunkInfo) sendData(ctx context.Context, address boson.Address, streamName string, msg interface{}) error {
	span, logger, ctx := ci.tracer.StartSpanFromContext(ctx, "pingpong-p2p-ping", ci.logger)
	defer span.Finish()

	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return fmt.Errorf("new stream: %w", err)
	}
	defer func() {
		go stream.FullClose()
	}()

	w, _ := protobuf.NewWriterAndReader(stream)
	if err := w.WriteMsgWithContext(ctx, &chunkInfoReq{rootCid: ""}); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	logger.Tracef("got chunkinfo req %q", msg)
	return nil
}

// sendDataToNode
func (ci *ChunkInfo) sendDataToNode(req interface{}, nodeId string) {
	//  todo libp2p
}

func (ci *ChunkInfo) handlerChunkInfoReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	_, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	span, logger, ctx := ci.tracer.StartSpanFromContext(ctx, "chunkinfo-p2p-handler", ci.logger)
	defer span.Finish()
	var req chunkInfoReq
	for {
		if err := r.ReadMsgWithContext(ctx, &req); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read chunkinfo req message: %w", err)
		}
		logger.Tracef("got chunkinfo req: %q", req)
		ci.onChunkInfoReq(nil, "", req)
	}
	return nil
}

func (ci *ChunkInfo) handlerChunkInfoResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	_, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	span, logger, ctx := ci.tracer.StartSpanFromContext(ctx, "chunkinfo-p2p-handler", ci.logger)
	defer span.Finish()
	var resp chunkInfoResp
	for {
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read message: %w", err)
		}
		logger.Tracef("got chunkinfo resp: %q", resp)
		ci.onChunkInfoResp(nil, "", resp)
	}
	return nil
}

func (ci *ChunkInfo) handlerPyramidReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	_, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	span, logger, ctx := ci.tracer.StartSpanFromContext(ctx, "chunkinfo-p2p-handler", ci.logger)
	defer span.Finish()
	var req chunkPyramidReq
	for {
		if err := r.ReadMsgWithContext(ctx, &req); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read message: %w", err)
		}
		logger.Tracef("got pyramid req: %q", req)
		// todo  onChunkPyramidReq
		ci.onChunkPyramidReq(nil, "", req)
	}
	return nil
}

func (ci *ChunkInfo) handlerPyramidResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	_, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	span, logger, ctx := ci.tracer.StartSpanFromContext(ctx, "chunkinfo-p2p-handler", ci.logger)
	defer span.Finish()
	var resp chunkPyramidResp
	for {
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read message: %w", err)
		}
		logger.Tracef("got pyramid resp: %q", resp)
		ci.onChunkPyramidResp(nil, "", resp)
	}
	return nil
}

// onChunkInfoReq
func (ci *ChunkInfo) onChunkInfoReq(authInfo []byte, nodeId string, body interface{}) {
	req := body.(chunkInfoReq)
	ctn := ci.ct.getNeighborChunkInfo(req.rootCid)
	resp := ci.ct.createChunkInfoResp(req.rootCid, ctn)
	ci.sendDataToNode(resp, nodeId)
}

// onChunkInfoResp
func (ci *ChunkInfo) onChunkInfoResp(authInfo []byte, nodeId string, body interface{}) {
	resp := body.(chunkInfoResp)
	ci.onFindChunkInfo(authInfo, resp.rootCid, nodeId, resp.presence)
}

// onChunkPyramidReq
func (ci *ChunkInfo) onChunkPyramidReq(authInfo []byte, nodeId string, req chunkPyramidReq) {
	cp := ci.ct.getChunkPyramid(req.RootCid)
	nci := ci.ct.getNeighborChunkInfo(req.RootCid)
	resp := ci.ct.createChunkPyramidResp(req.RootCid, cp, nci)
	ci.sendDataToNode(resp, nodeId)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(authInfo []byte, node string, body interface{}) {
	resp := body.(chunkPyramidResp)
	py := make(map[string]map[string]uint, len(resp.pyramid))
	pyz := make(map[string]uint, len(resp.pyramid))
	cn := make(map[string][]string)

	for _, cpr := range resp.pyramid {
		if cpr.nodes != nil && len(cpr.nodes) > 0 {
			cn[cpr.cid] = cpr.nodes
		}
		if py[cpr.pCid] == nil {
			pyz = make(map[string]uint, len(resp.pyramid))
		}
		pyz[cpr.cid] = cpr.order
		py[cpr.pCid] = pyz
	}
	ci.onFindChunkPyramid(authInfo, resp.rootCid, node, py, cn)
}

// onFindChunkPyramid
func (ci *ChunkInfo) onFindChunkPyramid(authInfo []byte, rootCid, node string, pyramids map[string]map[string]uint, cn map[string][]string) {
	_, ok := ci.cp.pyramid[rootCid]
	if !ok {
		// todo validate pyramid
		ci.cp.updateChunkPyramid(pyramids)
	}
	ci.onFindChunkInfo(authInfo, rootCid, node, cn)
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(authInfo []byte, rootCid, node string, chunkInfo map[string][]string) {
	ci.tt.removeTimeOutTrigger(rootCid, node)
	// todo Light Node
	nodes := make([]string, 0)
	for _, n := range chunkInfo {
		// todo validate rootCid:cid
		nodes = append(nodes, n...)
	}
	ci.cd.updateChunkInfos(rootCid, chunkInfo)
	ci.updateQueue(authInfo, rootCid, node, nodes)
}
