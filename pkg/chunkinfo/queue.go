package chunkinfo

import (
	"bytes"
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"sync"
)

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
	UnPull  []*[]byte
	Pulling []*[]byte
	Pulled  []*[]byte
}

// newQueue
func (ci *ChunkInfo) newQueue(rootCid string) {
	ci.queuesLk.Lock()
	defer ci.queuesLk.Unlock()
	q := &queue{
		UnPull:  make([]*[]byte, 0, PullerMax),
		Pulling: make([]*[]byte, 0, PullingMax),
		Pulled:  make([]*[]byte, 0, PullMax),
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
func (q *queue) peek(pull Pull) *[]byte {
	q.RLock()
	defer q.RLock()
	qu := q.getPull(pull)
	return qu[0]
}

// pop
func (q *queue) pop(pull Pull) *[]byte {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	v := qu[0]
	qu = qu[1:]
	q.updatePull(pull, qu)
	return v
}

// popNode
func (q *queue) popNode(pull Pull, overlay []byte) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	for i, n := range qu {
		f := *n
		if bytes.Equal(f, overlay) {
			qu = append(qu[:i], qu[i+1:]...)
		}
	}
	q.updatePull(pull, qu)
}

// push
func (q *queue) push(pull Pull, overlay []byte) {
	q.Lock()
	defer q.Unlock()
	qu := q.getPull(pull)
	qu = append(qu, &overlay)
	q.updatePull(pull, qu)
}

// getPull
func (q *queue) getPull(pull Pull) []*[]byte {
	switch pull {
	case UnPull:
		return q.UnPull
	case Pulling:
		return q.Pulling
	case Pulled:
		return q.Pulled
	}
	return make([]*[]byte, 0)
}

// updatePull
func (q *queue) updatePull(pull Pull, queue []*[]byte) {
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
	ci.queuesLk.RLock()
	defer ci.queuesLk.RUnlock()
	return ci.queues[rootCid]
}

// isExists
func (q *queue) isExists(pull Pull, overlay []byte) bool {
	q.RLock()
	defer q.RUnlock()
	pullNodes := q.getPull(pull)
	for _, pn := range pullNodes {
		f := *pn
		if bytes.Equal(f, overlay) {
			return true
		}
	}
	return false
}

// queueProcess
func (ci *ChunkInfo) queueProcess(ctx context.Context, rootCid boson.Address, streamName string) {
	// todo pyramid 不在走队列
	q := ci.getQueue(rootCid.String())
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
	ci.queuesLk.Lock()
	n := PullingMax - pullingLen
	ci.queuesLk.Unlock()
	if n > q.len(UnPull) {
		n = q.len(UnPull)
	}
	for i := 0; i < n; i++ {
		unNode := q.pop(UnPull)
		q.push(Pulling, *unNode)
		ci.tt.updateTimeOutTrigger(rootCid.Bytes(), *unNode)
		switch streamName {
		case streamChunkInfoReqName:
			ciReq := ci.cd.createChunkInfoReq(rootCid)
			ci.sendData(ctx, boson.NewAddress(*unNode), streamName, ciReq)
		case streamPyramidReqName:
			cpReq := ci.cp.createChunkPyramidReq(rootCid)
			ci.sendData(ctx, boson.NewAddress(*unNode), streamName, cpReq)
		}
	}
}

// updateQueue
func (ci *ChunkInfo) updateQueue(ctx context.Context, authInfo []byte, rootCid, overlay boson.Address, overlays [][]byte) {
	q := ci.getQueue(rootCid.String())
	for _, n := range overlays {
		if q.len(UnPull) >= PullerMax {
			break
		}
		if q.isExists(Pulled, n) || q.isExists(Pulling, n) || q.isExists(UnPull, n) {
			continue
		}
		q.push(UnPull, n)
	}
	q.popNode(Pulling, overlay.Bytes())
	q.push(Pulled, overlay.Bytes())
	ci.doFindChunkInfo(ctx, authInfo, rootCid)
}
