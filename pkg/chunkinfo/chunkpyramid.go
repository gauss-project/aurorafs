package chunk_info

import (
	"sync"
	"time"
)

// chunkPyramid 金字塔
type chunkPyramid struct {
	sync.RWMutex
	// 切片父id/切片id/切片所在树节点顺序
	pyramid map[string]map[string]uint
}

// chunkPyramidResp 金字塔响应体
type chunkPyramidResp struct {
	rootCid string
	pyramid []chunkPyramidChildResp
}

// chunkPyramidChildResp 金字塔响应体
type chunkPyramidChildResp struct {
	cid   string   // 切片id
	pCid  string   // 切片父id
	order uint     // 切片所在树节点顺序
	nodes []string //cid发现节点
}

// chunkPyramidReq 金字塔响应体
type chunkPyramidReq struct {
	rootCid    string
	createTime int64
}

// todo 验证金字塔结构是否正确

// updateChunkPyramid 新增金字塔
func (cp *chunkPyramid) updateChunkPyramid(pyramids map[string]map[string]uint) {
	cp.Lock()
	defer cp.Unlock()
	for key, pyramid := range pyramids {
		cp.pyramid[key] = pyramid
	}
}

// getChunkPyramid 根据rootCid获取金字塔
func (cp *chunkPyramid) getChunkPyramid(rootCid string) map[string]map[string]uint {
	cp.RLock()
	defer cp.RUnlock()
	ps := make(map[string]map[string]uint)
	cp.getChunkPyramidByPCid(rootCid, ps)
	return ps
}

// getChunkPyramidByPCid 获取金字塔结构
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

// createChunkPyramidReq 创建金字塔响应体
func (cp *chunkPyramid) createChunkPyramidReq(rootCid string) chunkPyramidReq {
	cpReq := chunkPyramidReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return cpReq
}

// getChunkPyramid 根据rootCid获取金字塔
func (cn *chunkInfoTabNeighbor) getChunkPyramid(rootCid string) map[string]map[string]uint {
	// todo 需要底层提供一个根据rootCid查询金字塔结构的接口
	// 组装成ChunkPyramid
	return make(map[string]map[string]uint)
}

// createChunkPyramidResp 创建金字塔响应体
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

// doFindChunkPyramid 发现树
func (ci *ChunkInfo) doFindChunkPyramid(authInfo []byte, rootCid string, nodes []string) {
	// 调用sendDataToNodes
	cpReq := ci.cp.createChunkPyramidReq(rootCid)
	for _, node := range nodes {
		// 定时任务
		ci.tt.updateTimeOutTrigger(rootCid, node)
		ci.sendDataToNode(cpReq, node)
	}
}
