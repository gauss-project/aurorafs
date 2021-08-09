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

// New 创建ChunkInfo
func New() *ChunkInfo {
	// message new
	cd := &chunkInfoDiscover{presence: make(map[string]map[string][]string)}
	ct := &chunkInfoTabNeighbor{presence: make(map[string][]string)}
	cp := &chunkPyramid{pyramid: map[string]map[string]uint{}}
	cpd := &pendingFinderInfo{finder: make(map[string]struct{})}
	queues := make(map[string]*queue)
	return &ChunkInfo{ct: ct, cd: cd, cp: cp, cpd: cpd, queues: queues}
}

// FindChunkInfo 根据rootCid与nodes开始发现
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

// GetChunkInfo 根据瑞rootCid与cid获取nodes
func (ci *ChunkInfo) GetChunkInfo(rootCid string, cid string) *[]string {
	return ci.cd.getChunkInfo(rootCid, cid)
}

// GetChunkPyramid 根据rootCid获取金字塔结构
func (ci *ChunkInfo) GetChunkPyramid(rootCid string) map[string]map[string]uint {
	return ci.cp.getChunkPyramid(rootCid)
}

// CancelFindChunkInfo 根据rootCid取消发现
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid string) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred 哪些node主动获取了cid
func (ci *ChunkInfo) OnChunkTransferred(cid string, rootCid string, node string) {
	ci.ct.updateNeighborChunkInfo(cid, rootCid, node)
}
