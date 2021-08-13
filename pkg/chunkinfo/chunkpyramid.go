package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
	"time"
)

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	// rootCid:cid
	pyramid map[string][][]byte
}

// todo validate pyramid

// updateChunkPyramid
func (cp *chunkPyramid) updateChunkPyramid(pyramids map[string][][]byte) {
	cp.Lock()
	defer cp.Unlock()
	for key, pyramid := range pyramids {
		cp.pyramid[key] = pyramid
	}
}

// createChunkPyramidReq
func (cp *chunkPyramid) createChunkPyramidReq(rootCid boson.Address) pb.ChunkPyramidReq {
	cpReq := pb.ChunkPyramidReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
	return cpReq
}

// getChunkPyramid
func (cn *chunkInfoTabNeighbor) getChunkPyramid(rootCid boson.Address) map[string][]byte {
	// todo getChunkPyramid
	return make(map[string][]byte)
}

// createChunkPyramidResp
func (cn *chunkInfoTabNeighbor) createChunkPyramidResp(rootCid boson.Address, cp map[string][]byte, ctn map[string]*pb.Overlays) pb.ChunkPyramidResp {
	return pb.ChunkPyramidResp{RootCid: rootCid.Bytes(), Pyramid: cp, Ctn: ctn}
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	cpReq := ci.cp.createChunkPyramidReq(rootCid)
	for _, overlay := range overlays {
		ci.tt.updateTimeOutTrigger(rootCid.Bytes(), overlay.Bytes())
		ci.sendData(ctx, overlay, streamPyramidReqName, cpReq)
	}
}
