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
	q := &queue{
		UnPull:  make([]*[]byte, 0, PullerMax),
		Pulling: make([]*[]byte, 0, PullingMax),
		Pulled:  make([]*[]byte, 0, PullMax),
	}
	ci.queues.Store(rootCid, q)
}

// len
func (q *queue) len(pull Pull) int {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return len(qu)
}

// peek
//func (q *queue) peek(pull Pull) *[]byte {
//	q.RLock()
//	defer q.RLock()
//	qu := q.getPull(pull)
//	return qu[0]
//}

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
	for i := 0; i < len(qu); i++ {
		f := *qu[i]
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
	var que *queue
	if q, ok := ci.queues.Load(rootCid); ok {
		que = q.(*queue)
	}
	return que
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
func (ci *ChunkInfo) queueProcess(ctx context.Context, rootCid boson.Address) {
	ci.queuesLk.Lock()
	defer ci.queuesLk.Unlock()
	q := ci.getQueue(rootCid.String())
	// pulled + pulling >= pullMax
	if q.len(Pulled)+q.len(Pulling) >= PullMax {
		return
	}
	if !ci.pendingFinder.getPendingFinder(rootCid) {
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
		overlay := boson.NewAddress(*unNode)
		ci.timeoutTrigger.updateTimeOutTrigger(rootCid.Bytes(), *unNode)
		ciReq := ci.createChunkInfoReq(rootCid, overlay, ci.addr)
		go func() {
			err := ci.sendDatas(ctx, overlay, streamChunkInfoReqName, ciReq)
			if err != nil {
				ci.logger.Errorf("[chunk info] send error :%v", err)
			}
		}()
	}
}

func (ci *ChunkInfo) updateQueue(rootCid, overlay boson.Address, chunkInfo map[string][]byte) {
	q := ci.getQueue(rootCid.String())
	if q == nil {
		return
	}
	for over := range chunkInfo {
		o := boson.MustParseHexAddress(over)
		n := o.Bytes()
		if o.Equal(ci.addr) {
			continue
		}
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
}
