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
	pyramid map[string]map[string]bool
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]bool)}
}

func (cp *chunkPyramid) checkPyramid(rootCid boson.Address, cid []byte) bool {
	cp.RLock()
	defer cp.RUnlock()
	if cp.pyramid[rootCid.String()] != nil {
		return cp.pyramid[rootCid.String()][boson.NewAddress(cid).String()]
	}
	return false
}

// updateChunkPyramid
func (cp *chunkPyramid) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte) {
	cp.Lock()
	defer cp.Unlock()
	py := make(map[string]bool)
	for _, p := range pyramids {
		for _, x := range p {
			py[boson.NewAddress(x).String()] = true
		}
	}
	cp.pyramid[rootCid.String()] = py
}

// createChunkPyramidReq
func (cp *chunkPyramid) createChunkPyramidReq(rootCid boson.Address) pb.ChunkPyramidReq {
	cpReq := pb.ChunkPyramidReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
	return cpReq
}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	v, err := ci.traversal.GetTrieData(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v, nil
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
