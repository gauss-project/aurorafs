package chunk_info

type Pull = uint32

const (
	UnPull Pull = iota
	Pulling
	Pulled
)

func (q *Queue) len(pull Pull) int {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return len(qu)
}

func (q *Queue) peek(pull Pull) *string {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return qu[0]
}

// Pop 出队
func (q *Queue) pop(pull Pull) *string {
	q.Lock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	v := qu[0]
	qu = qu[1:]
	q.updatePull(pull, qu)
	return v
}

func (q *Queue) popNode(pull Pull, node *string) {
	qu := q.getPull(pull)
	for i, n := range qu {
		if *n == *node {
			qu = append(qu[:i], qu[i+1:]...)
		}
	}
	q.updatePull(pull, qu)
}

// Push 入对
func (q *Queue) push(pull Pull, node *string) {
	q.Lock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	qu = append(qu, node)
	q.updatePull(pull, qu)
}

func (q *Queue) getPull(pull Pull) []*string {
	q.Lock()
	defer q.RUnlock()
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

func (q *Queue) updatePull(pull Pull, queue []*string) {
	switch pull {
	case UnPull:
		q.UnPull = queue
	case Pulling:
		q.Pulling = queue
	case Pulled:
		q.Pulled = queue
	}
}

func (q *Queue) isExists(pull Pull, node string) bool {
	pullNodes := q.getPull(pull)
	for _, pn := range pullNodes {
		if *pn == node {
			return true
		}
	}
	return false
}
