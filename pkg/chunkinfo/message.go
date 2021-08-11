package chunkinfo

// sendDataToNode
func (ci *ChunkInfo) sendDataToNode(req interface{}, nodeId string) {
	// todo libp2p
}

// onChunkInfoHandle
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

// onChunkInfoReq
func (ci *ChunkInfo) onChunkInfoReq(authInfo []byte, nodeId string, body interface{}) {
	req := body.(chunkInfoReq)
	ctn := ci.ct.getNeighborChunkInfo(req.rootCid)
	resp := ci.ct.createChunkInfoResp(req.rootCid, ctn)
	ci.sendDataToNode(resp, nodeId)
}

// onChunkInfoResp
func (ci *ChunkInfo) onChunkInfoResp(authInfo []byte, nodeId string, body interface{}) {
	resp := body.(chunkInfoResp)
	ci.onFindChunkInfo(authInfo, resp.rootCid, nodeId, resp.presence)
}

// onChunkPyramidReq
func (ci *ChunkInfo) onChunkPyramidReq(authInfo []byte, nodeId string, body interface{}) {
	req := body.(chunkPyramidReq)
	cp := ci.ct.getChunkPyramid(req.rootCid)
	nci := ci.ct.getNeighborChunkInfo(req.rootCid)
	resp := ci.ct.createChunkPyramidResp(req.rootCid, cp, nci)
	ci.sendDataToNode(resp, nodeId)
}

// onChunkPyramidResp
func (ci *ChunkInfo) onChunkPyramidResp(authInfo []byte, node string, body interface{}) {
	resp := body.(chunkPyramidResp)
	py := make(map[string]map[string]uint, len(resp.pyramid))
	pyz := make(map[string]uint, len(resp.pyramid))
	cn := make(map[string][]string)

	for _, cpr := range resp.pyramid {
		if cpr.nodes != nil && len(cpr.nodes) > 0 {
			cn[cpr.cid] = cpr.nodes
		}
		if py[cpr.pCid] == nil {
			pyz = make(map[string]uint, len(resp.pyramid))
		}
		pyz[cpr.cid] = cpr.order
		py[cpr.pCid] = pyz
	}
	ci.onFindChunkPyramid(authInfo, resp.rootCid, node, py, cn)
}

// onFindChunkPyramid
func (ci *ChunkInfo) onFindChunkPyramid(authInfo []byte, rootCid, node string, pyramids map[string]map[string]uint, cn map[string][]string) {
	_, ok := ci.cp.pyramid[rootCid]
	if !ok {
		// todo validate pyramid
		ci.cp.updateChunkPyramid(pyramids)
	}
	ci.onFindChunkInfo(authInfo, rootCid, node, cn)
}

// onFindChunkInfo
func (ci *ChunkInfo) onFindChunkInfo(authInfo []byte, rootCid, node string, chunkInfo map[string][]string) {
	ci.tt.removeTimeOutTrigger(rootCid, node)
	// todo Light Node
	nodes := make([]string, 0)
	for _, n := range chunkInfo {
		// todo validate rootCid:cid
		nodes = append(nodes, n...)
	}
	ci.cd.updateChunkInfos(rootCid, chunkInfo)
	ci.updateQueue(authInfo, rootCid, node, nodes)
}
