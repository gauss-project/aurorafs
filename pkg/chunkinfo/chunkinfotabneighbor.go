package chunk_info

import "sync"

type chunkInfoTabNeighbor struct {
	sync.RWMutex
	presence map[string][]string
}

type chunkInfoResp struct {
	rootCid  string              //rootCid
	presence map[string][]string // cid => nodes
}

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

func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid string, ctn map[string][]string) chunkInfoResp {
	return chunkInfoResp{rootCid: rootCid, presence: ctn}
}

func (cn *chunkInfoTabNeighbor) getChunkPyramid(rootCid string) map[string]map[string]uint {
	// todo 需要底层提供一个根据rootCid查询金字塔结构的接口
	// 组装成ChunkPyramid
	return make(map[string]map[string]uint)
}

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
