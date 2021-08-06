package chunk_info

func (cn *ChunkInfoTabNeighbor) updateNeighborChunkInfo(rootCid string, cid string, node string) {
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

func (cn *ChunkInfoTabNeighbor) getNeighborChunkInfo(rootCid string) map[string][]string {
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

func (cn *ChunkInfoTabNeighbor) createChunkInfoResp(rootCid string) ChunkInfoResp {
	cids := cn.getNeighborChunkInfo(rootCid)
	return ChunkInfoResp{rootCid: rootCid, presence: cids}
}

func (cn *ChunkInfoTabNeighbor) createChunkPyramidResp(rootCid string) []ChunkPyramidResp {
	// todo 需要底层提供一个根据rootCid查询金字塔结构的接口

	return nil
}

func (cn *ChunkInfoTabNeighbor) onChunkInfoReq(authInfo []byte, rootCid string, node string) {
	// 调用sendDataToNode
	ciResp := cn.createChunkInfoResp(rootCid)
	SendDataToNode(ciResp, node)
}
