package chunkinfo

import "sync"

type Pull = uint32

const (
	UnPull Pull = iota
	Pulling
	Pulled
)

const (
	PullMax    = 200
	PullingMax = 10
	PullerMax  = 1000
)

// queue
type queue struct {
	sync.RWMutex
	UnPull  []*string
	Pulling []*string
	Pulled  []*string
}

// newQueue
func (ci *ChunkInfo) newQueue(rootCid string) {
	q := &queue{
		UnPull:  make([]*string, 0, PullerMax),
		Pulling: make([]*string, 0, PullingMax),
		Pulled:  make([]*string, 0, PullMax),
	}
	ci.queues[rootCid] = q
}

// len
func (q *queue) len(pull Pull) int {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return len(qu)
}

// peek
func (q *queue) peek(pull Pull) *string {
	q.RLock()
	defer q.RLock()
	qu := q.getPull(pull)
	return qu[0]
}

// pop
func (q *queue) pop(pull Pull) *string {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	v := qu[0]
	qu = qu[1:]
	q.updatePull(pull, qu)
	return v
}

// popNode
func (q *queue) popNode(pull Pull, node string) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	for i, n := range qu {
		if *n == node {
			qu = append(qu[:i], qu[i+1:]...)
		}
	}
	q.updatePull(pull, qu)
}

// push
func (q *queue) push(pull Pull, node string) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	qu = append(qu, &node)
	q.updatePull(pull, qu)
}

// getPull
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

// updatePull
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

// getQueue
func (ci *ChunkInfo) getQueue(rootCid string) *queue {
	return ci.queues[rootCid]
}

// isExists
func (q *queue) isExists(pull Pull, node string) bool {
	q.RLock()
	defer q.RUnlock()
	pullNodes := q.getPull(pull)
	for _, pn := range pullNodes {
		if *pn == node {
			return true
		}
	}
	return false
}

// queueProcess
func (ci *ChunkInfo) queueProcess(rootCid string) {
	q := ci.getQueue(rootCid)
	// pulled + pulling >= pullMax
	if q.len(Pulled)+q.len(Pulling) >= PullMax {
		return
	}
	if !ci.cpd.getPendingFinder(rootCid) {
		return
	}
	pullingLen := q.len(Pulling)
	if pullingLen >= PullingMax {
		return
	}
	n := PullingMax - pullingLen
	if n > q.len(UnPull) {
		n = q.len(UnPull)
	}
	for i := 0; i < n; i++ {
		unNode := q.pop(UnPull)
		q.push(Pulling, *unNode)
		ci.tt.updateTimeOutTrigger(rootCid, *unNode)
		ciReq := ci.cd.createChunkInfoReq(rootCid)
		ci.sendDataToNode(ciReq, *unNode)
	}
}

// updateQueue
func (ci *ChunkInfo) updateQueue(authInfo []byte, rootCid, node string, nodes []string) {
	q := ci.getQueue(rootCid)
	for _, n := range nodes {
		if q.len(UnPull) >= PullerMax {
			return
		}
		if q.isExists(Pulled, n) || q.isExists(Pulling, n) || q.isExists(UnPull, n) {
			continue
		}
		q.push(UnPull, n)
	}
	q.popNode(Pulling, node)
	q.push(Pulled, node)
	ci.doFindChunkInfo(authInfo, rootCid)
}
