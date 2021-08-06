package chunk_info

type Pull = uint32

const (
	UnPull Pull = iota
	Pulling
	Pulled
)

func (q *Queue) Len(pull Pull) int {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return len(qu)
}

func (q *Queue) Peek(pull Pull) *string {
	q.RLock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	return qu[0]
}

func (q *Queue) Pop(pull Pull) *string {
	q.Lock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	v := qu[0]
	qu = qu[1:]
	q.updatePull(pull, qu)
	return v
}

func (q *Queue) Push(pull Pull, node *string) {
	q.Lock()
	defer q.RUnlock()
	qu := q.getPull(pull)
	qu = append(qu, node)
	q.updatePull(pull, qu)
}

func (q *Queue) getPull(pull Pull) []*string {
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

// pull 过程
