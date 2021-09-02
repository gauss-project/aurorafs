package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
)

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	// rootCid:cid
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
	py := make(map[string]int)
	for i, p := range pyramids {
		for _, x := range p {
			py[boson.NewAddress(x).String()] = i
		}
	}
	cp.pyramid[rootCid.String()] = py
}

// createChunkPyramidReq
//func (cp *chunkPyramid) createChunkPyramidReq(rootCid boson.Address) pb.ChunkPyramidReq {
//	cpReq := pb.ChunkPyramidReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
//	return cpReq
//}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	v, err := ci.traversal.GetTrieData(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cd *chunkPyramid) isExists(rootCid boson.Address) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.pyramid[rootCid.String()]
	return ok
}

func (ci *ChunkInfo) getChunkPyramidHash(cxt context.Context, rootCid boson.Address) ([][]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	resp := make([][]byte, 0)
	for k, _ := range v {
		resp = append(resp, []byte(k))
	}
	return resp, nil
}

func (ci *ChunkInfo) getChunkPyramidChunk(cxt context.Context, rootCid boson.Address, hash []byte) ([]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v[string(hash)], nil
}

// createChunkPyramidResp
//func (cn *chunkInfoTabNeighbor) createChunkPyramidResp(rootCid boson.Address, cp map[string][]byte) pb.ChunkPyramidResp {
//	// todo resp修改
//	return pb.ChunkPyramidResp{RootCid: rootCid.Bytes(), Pyramid: cp}
//}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlay boson.Address) error {
	// todo 不再走队列
	// 是否获到树
	if ci.cp.isExists(rootCid) {
		return nil
	}
	req := pb.ChunkPyramidHashReq{
		RootCid: rootCid.Bytes(),
	}
	resp, err := ci.sendPyramid(ctx, overlay, streamPyramidHashName, req)
	if err != nil {
		return err
	}
	return ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.RootCid), overlay, resp.(pb.ChunkPyramidHashResp))
}

func (cp *chunkPyramid) getChunkCid(rootCid boson.Address) [][]byte {
	v := cp.pyramid[rootCid.String()]
	cids := make([][]byte, 0)
	for overlay := range v {
		// 最底层cid
		cids = append(cids, boson.MustParseHexAddress(overlay).Bytes())
	}
	return cids
}

func (cp *chunkPyramid) getCidStore(rootCid, cid boson.Address) int {
	cp.RLock()
	defer cp.RUnlock()
	return cp.pyramid[rootCid.String()][cid.String()]
}
