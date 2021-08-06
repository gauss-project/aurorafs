package chunk_info

func SendDataToNode(req interface{}, node string) {
	// 调用libp2p发送请求
}

func OnChunkInfoHandle() {
	// todo 判断是req还是resp
	// todo 判断是chunkinfo还是chunkpyramid
}

func (ci *ChunkInfo) onChunkInfoReq() {
	// 根据rootcid获取最底一级的cids=>nodes
	// 调用createChunkInfoResp
	// 调用sendDataToNode
}

func (ci *ChunkInfo) onChunkInfoResp() {
	// 调用onFindChunkInfo
	ci.onFindChunkInfo("", nil)
}

func (ci *ChunkInfo) onChunkPyramidReq() {
	// 根据rootCid获取金字塔结构
	// 获取最底一级的cids=>node
	// 调用createChunkPyramidResp
	// 调用sendDataToNode
}

func (ci *ChunkInfo) onChunkPyramidResp() {
	// 调用onFindChunkPyramid
	ci.onFindChunkPyramid("", nil)
}

func (ci *ChunkInfo) onFindChunkPyramid(rootCid string, pyramids map[string]map[string]uint) {
	// 是否已经发现了
	_, ok := ci.cp.Pyramid[rootCid]
	if !ok {
		return
	}
	// todo 验证金字塔数据是否正确
	// 更新chunkpyramid
	ci.cp.UpdateChunkPyramid(pyramids)
	// 调用onFindChunkInfo
}

func (ci *ChunkInfo) onFindChunkInfo(rootCid string, pyramids map[string][]string) {
	// 检查是否有新节点是否为轻节点
	// 更新chunkinfodiscover
	ci.cd.UpdateChunkInfo(rootCid, pyramids)
	// 新节点到Unpull队列
	// 发送新节点通知
}
