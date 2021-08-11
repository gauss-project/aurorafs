package chunkinfo

import (
	"sync"
	"time"
)

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	// pCid:cid:order
	pyramid map[string]map[string]uint
}

// chunkPyramidResp
type chunkPyramidResp struct {
	rootCid string
	pyramid []chunkPyramidChildResp
}

// chunkPyramidChildResp
type chunkPyramidChildResp struct {
	cid   string
	pCid  string
	order uint
	nodes []string
}

// chunkPyramidReq
type chunkPyramidReq struct {
	rootCid    string
	createTime int64
}

// todo validate pyramid

// updateChunkPyramid
func (cp *chunkPyramid) updateChunkPyramid(pyramids map[string]map[string]uint) {
	cp.Lock()
	defer cp.Unlock()
	for key, pyramid := range pyramids {
		cp.pyramid[key] = pyramid
	}
}

// getChunkPyramid
func (cp *chunkPyramid) getChunkPyramid(rootCid string) map[string]map[string]uint {
	cp.RLock()
	defer cp.RUnlock()
	ps := make(map[string]map[string]uint)
	cp.getChunkPyramidByPCid(rootCid, ps)
	return ps
}

// getChunkPyramidByPCid
func (cp *chunkPyramid) getChunkPyramidByPCid(pCid string, pyramids map[string]map[string]uint) map[string]map[string]uint {
	cids, ok := cp.pyramid[pCid]
	if !ok {
		return pyramids
	}
	pyramids[pCid] = cids
	for cid, _ := range cids {
		//  tree
		return cp.getChunkPyramidByPCid(cid, pyramids)
	}
	return pyramids
}

// createChunkPyramidReq
func (cp *chunkPyramid) createChunkPyramidReq(rootCid string) chunkPyramidReq {
	cpReq := chunkPyramidReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return cpReq
}

// getChunkPyramid
func (cn *chunkInfoTabNeighbor) getChunkPyramid(rootCid string) map[string]map[string]uint {
	// todo getChunkPyramid
	return make(map[string]map[string]uint)
}

// createChunkPyramidResp
func (cn *chunkInfoTabNeighbor) createChunkPyramidResp(rootCid string, cp map[string]map[string]uint, ctn map[string][]string) chunkPyramidResp {
	resp := make([]chunkPyramidChildResp, 0)
	for k, v := range cp {
		for pk, pv := range v {
			cpr := chunkPyramidChildResp{pk, k, pv, ctn[pk]}
			resp = append(resp, cpr)
		}
	}
	return chunkPyramidResp{rootCid: rootCid, pyramid: resp}
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(authInfo []byte, rootCid string, nodes []string) {
	cpReq := ci.cp.createChunkPyramidReq(rootCid)
	for _, node := range nodes {
		ci.tt.updateTimeOutTrigger(rootCid, node)
		ci.sendDataToNode(cpReq, node)
	}
}
