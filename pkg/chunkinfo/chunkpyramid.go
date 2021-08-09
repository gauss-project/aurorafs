package chunk_info

import (
	"sync"
	"time"
)

type chunkPyramid struct {
	sync.RWMutex
	// 切片父id/切片id/切片所在树节点顺序
	pyramid map[string]map[string]uint
}

type chunkPyramidResp struct {
	rootCid string
	pyramid []chunkPyramidChildResp
}

type chunkPyramidChildResp struct {
	cid   string   // 切片id
	pCid  string   // 切片父id
	order uint     // 切片所在树节点顺序
	nodes []string //cid发现节点
}
type chunkPyramidReq struct {
	rootCid    string
	createTime int64
}

// todo 验证金字塔结构是否正确

func (cp *chunkPyramid) updateChunkPyramid(pyramids map[string]map[string]uint) {
	cp.Lock()
	defer cp.RUnlock()
	for key, pyramid := range pyramids {
		cp.pyramid[key] = pyramid
	}
}

func (cp *chunkPyramid) getChunkPyramid(rootCid string) map[string]map[string]uint {
	cp.RLock()
	defer cp.RUnlock()
	ps := make(map[string]map[string]uint)
	cp.getChunkPyramidByPCid(rootCid, ps)
	return ps
}

func (cp *chunkPyramid) getChunkPyramidByPCid(pCid string, pyramids map[string]map[string]uint) map[string]map[string]uint {
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

func (cp *chunkPyramid) createChunkPyramidReq(rootCid string) chunkPyramidReq {
	cpReq := chunkPyramidReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return cpReq
}
