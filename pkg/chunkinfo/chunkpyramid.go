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
	// todo 金字塔修改
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
}

// todo init 从数据库添加数据

func (cp *chunkPyramid) checkPyramid(rootCid, cid boson.Address) bool {
	cp.RLock()
	defer cp.RUnlock()
	if cp.pyramid[rootCid.String()] != nil {
		_, ok := cp.pyramid[rootCid.String()][cid.String()]
		return ok
	}
	return false
}

// updateChunkPyramid
func (cp *chunkPyramid) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte) {
	cp.Lock()
	defer cp.Unlock()
	// todo 逻辑修改
	py := make(map[string]int)
	for _, p := range pyramids {
		for _, x := range p {
			py[boson.NewAddress(x).String()] = 1
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
func (cn *chunkInfoTabNeighbor) createChunkPyramidResp(rootCid boson.Address, cp map[string][]byte) pb.ChunkPyramidResp {
	// todo resp修改
	return pb.ChunkPyramidResp{RootCid: rootCid.Bytes(), Pyramid: cp}
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	// todo 不再走队列
	ci.queueProcess(ctx, rootCid, streamPyramidReqName)
}

func (ci *ChunkInfo) getChunkCid(ctx context.Context, rootCid boson.Address) [][]byte {
	tree, _ := ci.getChunkPyramid(ctx, rootCid)
	v, _ := ci.traversal.CheckTrieData(ctx, rootCid, tree)
	cids := make([][]byte, 0)
	for _, p := range v {
		// 最底层cid
		cids = append(cids, p...)
	}
	return cids
}

func (cp *chunkPyramid) getCidStore(rootCid, cid boson.Address) int {
	cp.RLock()
	defer cp.RUnlock()
	return cp.pyramid[rootCid.String()][cid.String()]
}
