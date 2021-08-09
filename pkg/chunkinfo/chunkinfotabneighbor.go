package chunk_info

import "sync"

// chunkInfoTabNeighbor 哪些节点主动获取过当前节点Chunk记录
type chunkInfoTabNeighbor struct {
	sync.RWMutex
	// 1. cid 对应节点 2. rootCid对应cid
	presence map[string][]string
}

// chunkInfoResp chunkInfo 响应体
type chunkInfoResp struct {
	rootCid  string              //rootCid
	presence map[string][]string // cid => nodes
}

// updateNeighborChunkInfo 新增NeighborChunkInfo
func (cn *chunkInfoTabNeighbor) updateNeighborChunkInfo(rootCid string, cid string, node string) {
	cn.Lock()
	defer cn.Unlock()
	// todo 数据库操作
	_, ok := cn.presence[rootCid]
	if !ok {
		cn.presence[rootCid] = make([]string, 1)
	}
	key := rootCid + "_" + cid
	_, pok := cn.presence[key]
	if !pok {
		cn.presence[key] = make([]string, 1)
		cn.presence[rootCid] = append(cn.presence[rootCid], cid)
	}
	cn.presence[key] = append(cn.presence[key], node)
}

// getNeighborChunkInfo 根据rootCid 获取下面所有所有cid对应nodes
func (cn *chunkInfoTabNeighbor) getNeighborChunkInfo(rootCid string) map[string][]string {
	cn.RLock()
	defer cn.RUnlock()
	var res map[string][]string
	cids := cn.presence[rootCid]
	for _, cid := range cids {
		key := rootCid + "_" + cid
		// todo 数据库操作
		nodes := cn.presence[key]
		res[cid] = nodes
	}
	return res
}

// createChunkInfoResp 创建chunkinfo 响应体
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid string, ctn map[string][]string) chunkInfoResp {
	return chunkInfoResp{rootCid: rootCid, presence: ctn}
}
