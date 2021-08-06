package handle

func OnChunkInfoHandle() {
	// todo 判断是req还是resp
	// todo 判断是chunkinfo还是chunkpyramid
}

func onChunkInfoReq() {
	// 根据rootcid获取最底一级的cids=>nodes
	// 调用createChunkInfoResp
	// 调用sendDataToNode
}

func onChunkInfoResp() {
	// 调用onFindChunkInfo
}

func onChunkPyramidReq() {
	// 根据rootCid获取金字塔结构并获取最底一级的cids=>node
	// 调用createChunkPyramidResp
	// 调用sendDataToNode
}

func onChunkPyramidResp() {
	// 调用onFindChunkPyramid
}
