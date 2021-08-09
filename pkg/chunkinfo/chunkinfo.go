package chunk_info

// ChunkInfo 主要属性
type ChunkInfo struct {
	// todo 定时器
	queues map[string]*queue     // 队列
	ct     *chunkInfoTabNeighbor // 哪些节点主动获取过当前节点Chunk记录
	cd     *chunkInfoDiscover    // 当前节点知道哪些chunk在哪些节点上
	cp     *chunkPyramid         // 金字塔结构
	cpd    *pendingFinderInfo    // 是否发现RootCid
}

func New() *ChunkInfo {
	// message new
	cd := &chunkInfoDiscover{presence: make(map[string]map[string][]string)}
	ct := &chunkInfoTabNeighbor{presence: make(map[string][]string)}
	cp := &chunkPyramid{pyramid: map[string]map[string]uint{}}
	cpd := &pendingFinderInfo{finder: make(map[string]struct{})}
	queues := make(map[string]*queue)
	return &ChunkInfo{ct: ct, cd: cd, cp: cp, cpd: cpd, queues: queues}
}

func (ci *ChunkInfo) FindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	//  如果已经存在rootCid并且未开始发现直接发起doFindChunkInfo
	if ci.cd.isExists(rootCid) {
		if ci.cpd.getPendingFinder(rootCid) {
			return
		}
		//发起doFindChunkInfo
		for _, n := range nodes {
			ci.queues[rootCid].push(UnPull, &n)
		}
		ci.doFindChunkInfo(authInfo, rootCid)
	} else {
		// 根据rootCid生成队列
		ci.newQueue(rootCid)
		// 获取金字塔
		ci.doFindChunkPyramid(authInfo, rootCid, nodes)
	}
}

func (ci *ChunkInfo) GetChunkInfo(rootCid string, cid string) *[]string {
	return ci.cd.getChunkInfo(rootCid, cid)
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid string) map[string]map[string]uint {
	return ci.cp.getChunkPyramid(rootCid)
}

func (ci *ChunkInfo) CancelFindChunkInfo(rootCid string) {
	ci.cpd.cancelPendingFinder(rootCid)
}

func (ci *ChunkInfo) OnChunkTransferred(cid string, rootCid string, node string) {
	ci.ct.updateNeighborChunkInfo(cid, rootCid, node)
}

func (ci *ChunkInfo) doFindChunkInfo(authInfo []byte, rootCid string) {
	// pull 过程
	ci.queueProcess(rootCid)

}

func (ci *ChunkInfo) doFindChunkPyramid(authInfo []byte, rootCid string, nodes []string) {
	// 调用sendDataToNodes
	cpReq := ci.cp.createChunkPyramidReq(rootCid)
	for _, node := range nodes {
		ci.sendDataToNode(cpReq, node)
	}
}
