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
	queues []*Queue
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
	RootCid string
	Puller  []string
	UnPull  []*string
	Pulling []*string
	Pulled  []*string
}

type ChunkPyramid struct {
	sync.RWMutex
	// 切片父id/切片id/切片所在树节点顺序
	Pyramid map[string]map[string]uint
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
	cp := &ChunkPyramid{Pyramid: map[string]map[string]uint{}}
	cpd := &PendingFinderInfo{finder: make(map[string]struct{})}
	queues := make([]*Queue, 0)
	return &ChunkInfo{ct: ct, cd: cd, cp: cp, cpd: cpd, queues: queues}
}

func NewQueue(rootCid string) *Queue {
	return &Queue{
		RootCid: rootCid,
		Puller:  make([]string, 0, PullerMax),
		UnPull:  make([]*string, 0, PullerMax),
		Pulling: make([]*string, 0, PullingMax),
		Pulled:  make([]*string, 0, PullMax),
	}
}

func (ci *ChunkInfo) FindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	//  如果已经存在rootCid并且未开始发现直接发起doFindChunkInfo
	if ci.cd.isExists(rootCid) {
		if ci.cpd.getPendingFinder(rootCid) {
			return
		}
		//发起doFindChunkInfo
		ci.cd.doFindChunkInfo(authInfo, rootCid, nodes)
	} else {
		// 根据rootCid生成队列
		queue := NewQueue(rootCid)
		ci.queues = append(ci.queues, queue)
		// 获取金字塔
		ci.cp.doFindChunkPyramid(authInfo, rootCid, nodes)
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
	for _, queue := range ci.queues {
		if queue.RootCid == rootCid {
			return queue
		}
	}
	return nil
}
