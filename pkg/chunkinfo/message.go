package chunk_info

// sendDataToNode 发送请求
func (ci *ChunkInfo) sendDataToNode(req interface{}, nodeId string) {
	// 调用libp2p发送请求
}

// onChunkInfoHandle 接受请求
func (ci *ChunkInfo) onChunkInfoHandle(authInfo []byte, cmd, nodeId string, body interface{}) {
	if cmd == "req/chunkinfo" {
		ci.onChunkInfoReq(authInfo, nodeId, body)
	}
	if cmd == "resp/chunkinfo" {
		ci.onChunkInfoResp(authInfo, nodeId, body)
	}
	if cmd == "req/chunkpyramid" {
		ci.onChunkPyramidReq(authInfo, nodeId, body)
	}
	if cmd == "resp/chunkpyramid" {
		ci.onChunkPyramidResp(authInfo, nodeId, body)
	}
}

// onChunkInfoReq 获取其他节点chunkinfo req请求
func (ci *ChunkInfo) onChunkInfoReq(authInfo []byte, nodeId string, body interface{}) {
	// 根据rootcid获取最底一级的cids=>nodes
	req := body.(chunkInfoReq)
	ctn := ci.ct.getNeighborChunkInfo(req.rootCid)
	// 调用createChunkInfoResp
	resp := ci.ct.createChunkInfoResp(req.rootCid, ctn)
	// 调用sendDataToNode
	ci.sendDataToNode(resp, nodeId)
}

// onChunkInfoResp 获取其他节点响应请求
func (ci *ChunkInfo) onChunkInfoResp(authInfo []byte, nodeId string, body interface{}) {
	resp := body.(chunkInfoResp)
	ci.onFindChunkInfo(authInfo, resp.rootCid, nodeId, resp.presence)
}

// onChunkPyramidReq 获取其他节点请求获取金字塔
func (ci *ChunkInfo) onChunkPyramidReq(authInfo []byte, nodeId string, body interface{}) {
	// 根据rootCid获取金字塔结构
	req := body.(chunkPyramidReq)
	cp := ci.ct.getChunkPyramid(req.rootCid)
	// 根据rootCid获取最后一层的节点
	nci := ci.ct.getNeighborChunkInfo(req.rootCid)
	resp := ci.ct.createChunkPyramidResp(req.rootCid, cp, nci)
	ci.sendDataToNode(resp, nodeId)
}

// onChunkPyramidResp 获取其他节点响应的金字塔请求
func (ci *ChunkInfo) onChunkPyramidResp(authInfo []byte, node string, body interface{}) {
	resp := body.(chunkPyramidResp)
	py := make(map[string]map[string]uint, len(resp.pyramid))
	pyz := make(map[string]uint, len(resp.pyramid))
	cn := make(map[string][]string)

	for _, cpr := range resp.pyramid {
		if cpr.nodes != nil && len(cpr.nodes) > 0 {
			cn[cpr.cid] = cpr.nodes
		}
		pyz[cpr.cid] = cpr.order
		py[cpr.pCid] = pyz
	}
	ci.onFindChunkPyramid(authInfo, resp.rootCid, node, py, cn)
}

// onFindChunkPyramid  金字塔请求流程
func (ci *ChunkInfo) onFindChunkPyramid(authInfo []byte, rootCid, node string, pyramids map[string]map[string]uint, cn map[string][]string) {
	// 是否已经发现了
	_, ok := ci.cp.pyramid[rootCid]
	if !ok {
		// todo 验证金字塔数据是否正确
		ci.cp.updateChunkPyramid(pyramids)
	}
	ci.onFindChunkInfo(authInfo, rootCid, node, cn)
}

// onFindChunkInfo chunkinfo 请求流程
func (ci *ChunkInfo) onFindChunkInfo(authInfo []byte, rootCid, node string, chunkInfo map[string][]string) {
	// 请求成功关闭超时监听
	ci.tt.removeTimeOutTrigger(rootCid, node)
	// todo 检查是否有新节点是否为轻节点
	// 更新chunkinfodiscover
	nodes := make([]string, 0)
	for _, n := range chunkInfo {
		nodes = append(nodes, n...)
	}
	ci.cd.updateChunkInfos(rootCid, chunkInfo)
	ci.updateQueue(authInfo, rootCid, node, nodes)
}
