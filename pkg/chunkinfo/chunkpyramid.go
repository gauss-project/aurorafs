package chunk_info

import (
	"time"
)

// todo 验证金字塔结构是否正确

func (cp *ChunkPyramid) updateChunkPyramid(pyramids map[string]map[string]uint) {
	cp.Lock()
	defer cp.RUnlock()
	for key, pyramid := range pyramids {
		cp.pyramid[key] = pyramid
	}
}

func (cp *ChunkPyramid) getChunkPyramid(rootCid string) map[string]map[string]uint {
	cp.RLock()
	defer cp.RUnlock()
	ps := make(map[string]map[string]uint)
	cp.getChunkPyramidByPCid(rootCid, ps)
	return ps
}

func (cp *ChunkPyramid) getChunkPyramidByPCid(pCid string, pyramids map[string]map[string]uint) map[string]map[string]uint {
	cids, ok := cp.pyramid[pCid]
	if !ok {
		return pyramids
	}
	pyramids[pCid] = cids
	for cid, _ := range cids {
		// todo tree
		return cp.getChunkPyramidByPCid(cid, pyramids)
	}
	return pyramids
}

func (cp *ChunkPyramid) createChunkPyramidReq(rootCid string) ChunkPyramidReq {
	cpReq := ChunkPyramidReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return cpReq
}
