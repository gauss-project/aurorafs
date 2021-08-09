package chunk_info

import "sync"

const (
	//PullMax 最大拉取数 pulling+pulled
	PullMax = 200
	//PullingMax 最大并行拉取数
	PullingMax = 10
	//PullerMax 为队列拉取最大数
	PullerMax = 1000
)

type ChunkInfo struct {
	// 定时器
	queues map[string]*Queue
	ct     *ChunkInfoTabNeighbor
	cd     *ChunkInfoDiscover
	cp     *ChunkPyramid
	cpd    *PendingFinderInfo
}

type PendingFinderInfo struct {
	sync.RWMutex
	finder map[string]struct{}
}

type Queue struct {
	sync.RWMutex
	UnPull  []*string
	Pulling []*string
	Pulled  []*string
}

type ChunkPyramid struct {
	sync.RWMutex
	// 切片父id/切片id/切片所在树节点顺序
	pyramid map[string]map[string]uint
}

type ChunkInfoTabNeighbor struct {
	sync.RWMutex
	presence map[string][]string
}

type ChunkInfoDiscover struct {
	sync.RWMutex
	presence map[string]map[string][]string
}

type ChunkInfoReq struct {
	rootCid    string
	createTime int64
}

type ChunkInfoResp struct {
	rootCid  string              //rootCid
	presence map[string][]string // cid => nodes
}

type ChunkPyramidResp struct {
	rootCid string
	pyramid []ChunkPyramidChildResp
}

type ChunkPyramidChildResp struct {
	cid   string   // 切片id
	pCid  string   // 切片父id
	order uint     // 切片所在树节点顺序
	nodes []string //cid发现节点
}
type ChunkPyramidReq struct {
	rootCid    string
	createTime int64
}

func New() *ChunkInfo {
	// message new
	cd := &ChunkInfoDiscover{presence: make(map[string]map[string][]string)}
	ct := &ChunkInfoTabNeighbor{presence: make(map[string][]string)}
	cp := &ChunkPyramid{pyramid: map[string]map[string]uint{}}
	cpd := &PendingFinderInfo{finder: make(map[string]struct{})}
	queues := make(map[string]*Queue)
	return &ChunkInfo{ct: ct, cd: cd, cp: cp, cpd: cpd, queues: queues}
}

func (ci *ChunkInfo) newQueue(rootCid string) {
	q := &Queue{
		//Puller:  make([]string, 0, PullerMax),
		UnPull:  make([]*string, 0, PullerMax),
		Pulling: make([]*string, 0, PullingMax),
		Pulled:  make([]*string, 0, PullMax),
	}
	ci.queues[rootCid] = q
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

func (ci *ChunkInfo) getQueue(rootCid string) *Queue {
	return ci.queues[rootCid]
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

// pull 过程
func (ci *ChunkInfo) queueProcess(rootCid string) {
	q := ci.getQueue(rootCid)
	// pulled + pulling >= pullMax
	if q.len(Pulled)+q.len(Pulling) >= PullMax {
		return
	}
	// 判断正在查是否等于最大查询数
	pullingLen := q.len(Pulling)
	if pullingLen >= PullingMax {
		return
	}
	// 最大查询数-当前正在查询数=n
	n := PullingMax - pullingLen
	// 放入n个节点到正在查询
	for i := 0; i < n; i++ {
		unNode := q.pop(UnPull)
		q.push(Pulling, unNode)
		// todo 定时器 对请求超时做处理
		ciReq := ci.cd.createChunkInfoReq(rootCid)
		ci.sendDataToNode(ciReq, *unNode)
	}
}

func (ci *ChunkInfo) updateQueue(authInfo []byte, rootCid, node string, nodes []string) {
	q := ci.queues[rootCid]
	for _, n := range nodes {
		// 填充到未查询队列, 如果最大拉取数则不在填充到为查询队列中
		if q.len(UnPull) >= PullerMax {
			return
		}
		// 是否为新节点
		if q.isExists(Pulled, n) || q.isExists(Pulling, n) || q.isExists(UnPull, n) {
			continue
		}
		q.push(UnPull, &n)
	}
	// 节点从正在查转为已查询
	q.popNode(Pulling, &node)
	q.push(Pulled, &node)
	// 触发新节点通知
	ci.doFindChunkInfo(authInfo, rootCid)
}
