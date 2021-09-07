package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"io"
	"time"
)

const (
	protocolName            = "chunkinfo"
	protocolVersion         = "1.0.0"
	streamChunkInfoReqName  = "chunkinforeq"
	streamChunkInfoRespName = "chunkinforesp"
	streamPyramidHashName   = "chunkpyramidhash"
	streamPyramidChunkName  = "chunkpyramidchunk"
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

func (ci *ChunkInfo) sendData(ctx context.Context, address boson.Address, streamName string, msg interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, TimeOut*time.Second)
	defer cancel()
	if err := ci.route.Connect(ctx, address); err != nil {
		return err
	}
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
	}

	ci.logger.Tracef("got chunk info req %q", msg)
	return nil
}

func (ci *ChunkInfo) sendPyramid(ctx context.Context, address boson.Address, streamName string, msg interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, TimeOut*time.Second)
	defer cancel()
	if err := ci.route.Connect(ctx, address); err != nil {
		return nil, err
	}
	stream, err := ci.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		ci.logger.Errorf("new stream: %w", err)
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
			return nil, fmt.Errorf("write message: %w", err)
		}
		var resp pb.ChunkPyramidHashResp
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read message: %w", err)
		}
		return resp, nil
	case streamPyramidChunkName:
		req := msg.(pb.ChunkPyramidChunkReq)
		if err := w.WriteMsgWithContext(ctx, &req); err != nil {
			return nil, fmt.Errorf("write message: %w", err)
		}
		var resp pb.ChunkPyramidChunkResp
		if err := r.ReadMsgWithContext(ctx, &resp); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read message: %w", err)
		}
		return resp, nil
	}
	return nil, nil
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

func (ci *ChunkInfo) handlerPyramidHash(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidHashReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[chunk info] read pyramid hash message: %w", err)
		return fmt.Errorf("[chunk info] read pyramid hash message: %w", err)
	}
	ci.logger.Tracef("[chunk info]  got pyramid req: %q", req)

	hashs, err := ci.onChunkPyramidHashReq(ctx, nil, req)
	if err != nil {
		return err
	}
	var resp = pb.ChunkPyramidHashResp{Hash: hashs}
	if err := w.WriteMsgWithContext(ctx, &resp); err != nil {
		return fmt.Errorf("[chunk info] write hash message: %w", err)
	}
	return nil
}

func (ci *ChunkInfo) handlerPyramidChunk(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {

	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.FullClose()
	var req pb.ChunkPyramidChunkReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		ci.logger.Errorf("[chunk info] read pyramid hash message: %w", err)
		return fmt.Errorf("[chunk info] read pyramid hash message: %w", err)
	}
	ci.logger.Tracef("[chunk info]  got pyramid req: %q", req)

	chunks, err := ci.onChunkPyramidChunkReq(ctx, nil, req)
	if err != nil {
		return err
	}
	var resp = pb.ChunkPyramidChunkResp{Chunk: chunks}
	if err := w.WriteMsgWithContext(ctx, &resp); err != nil {
		return fmt.Errorf("[chunk info] write hash message: %w", err)
	}

	return nil
}

// onChunkInfoReq
func (ci *ChunkInfo) onChunkInfoReq(ctx context.Context, authInfo []byte, overlay boson.Address, req pb.ChunkInfoReq) error {
	rc := boson.NewAddress(req.RootCid)
	ctn := ci.ct.getNeighborChunkInfo(rc)
	resp := ci.ct.createChunkInfoResp(rc, ctn)
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
		}
		v, err := ci.sendPyramid(ctx, overlay, streamPyramidChunkName, req)
		if err != nil {
			return err
		}
		resp := v.(pb.ChunkPyramidChunkResp)
		pyramid[string(hash)] = resp.Chunk
	}

	v, err := ci.traversal.CheckTrieData(ctx, rootCid, pyramid)
	if err != nil {
		ci.logger.Errorf("chunk pyramid: check pyramid error")
		return err
	}
	ci.updateChunkPyramid(rootCid, v, hashs)
	ci.ct.initNeighborChunkInfo(rootCid)
	return nil
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, chunkInfo map[string][]byte) {
	ci.tt.removeTimeOutTrigger(rootCid, overlay)
	overlays := make([][]byte, 0, len(chunkInfo))
	for over, bv := range chunkInfo {
		overlays = append(overlays, boson.MustParseHexAddress(over).Bytes())
		ci.updateChunkInfo(rootCid, boson.MustParseHexAddress(over), bv)
	}
	ci.updateQueue(ctx, authInfo, rootCid, overlay, overlays)
}
