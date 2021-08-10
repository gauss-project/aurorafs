package chunk_info

import "sync"

type Pull = uint32

const (
	UnPull Pull = iota
	Pulling
	Pulled
)

const (
	//PullMax 最大拉取数 pulling+pulled
	PullMax = 200
	//PullingMax 最大并行拉取数
	PullingMax = 10
	//PullerMax 为队列拉取最大数
	PullerMax = 1000
)

// queue 拉取队列
type queue struct {
	sync.RWMutex
	UnPull  []*string // 未拉取队列
	Pulling []*string // 正在拉取队列
	Pulled  []*string // 已拉取队列
}

// newQueue 创建拉取队列
func (ci *ChunkInfo) newQueue(rootCid string) {
	q := &queue{
		UnPull:  make([]*string, 0, PullerMax),
		Pulling: make([]*string, 0, PullingMax),
		Pulled:  make([]*string, 0, PullMax),
	}
	ci.queues[rootCid] = q
}

// len 队列长度
func (q *queue) len(pull Pull) int {
	q.RLock()
	defer q.Unlock()
	qu := q.getPull(pull)
	return len(qu)
}

// peek 队列头节点
func (q *queue) peek(pull Pull) *string {
	q.RLock()
	defer q.Unlock()
	qu := q.getPull(pull)
	return qu[0]
}

// pop 出队
func (q *queue) pop(pull Pull) *string {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	v := qu[0]
	qu = qu[1:]
	q.updatePull(pull, qu)
	return v
}

// popNode 指定某一个node出对列
func (q *queue) popNode(pull Pull, node *string) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	for i, n := range qu {
		if *n == *node {
			qu = append(qu[:i], qu[i+1:]...)
		}
	}
	q.updatePull(pull, qu)
}

// push 入对
func (q *queue) push(pull Pull, node *string) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	qu = append(qu, node)
	q.updatePull(pull, qu)
}

// getPull 根据拉取类型获取队列
func (q *queue) getPull(pull Pull) []*string {
	switch pull {
	case UnPull:
		return q.UnPull
	case Pulling:
		return q.Pulling
	case Pulled:
		return q.Pulled
	}
	return make([]*string, 0)
}

// updatePull 根据对垒类型修改队列
func (q *queue) updatePull(pull Pull, queue []*string) {
	switch pull {
	case UnPull:
		q.UnPull = queue
	case Pulling:
		q.Pulling = queue
	case Pulled:
		q.Pulled = queue
	}
}

// getQueue 根据rootCid获取队列
func (ci *ChunkInfo) getQueue(rootCid string) *queue {
	return ci.queues[rootCid]
}

// 根据队列类型判断节点是否存在
func (q *queue) isExists(pull Pull, node string) bool {
	q.RLock()
	defer q.Unlock()
	pullNodes := q.getPull(pull)
	for _, pn := range pullNodes {
		if *pn == node {
			return true
		}
	}
	return false
}

// pull 过程
func (ci *ChunkInfo) queueProcess(rootCid string) {
	q := ci.getQueue(rootCid)
	q.Lock()
	defer q.Unlock()
	// pulled + pulling >= pullMax
	if q.len(Pulled)+q.len(Pulling) >= PullMax {
		return
	}
	// 判断是否取消发现
	if !ci.cpd.getPendingFinder(rootCid) {
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

// updateQueue 修改队列
func (ci *ChunkInfo) updateQueue(authInfo []byte, rootCid, node string, nodes []string) {
	q := ci.getQueue(rootCid)
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
