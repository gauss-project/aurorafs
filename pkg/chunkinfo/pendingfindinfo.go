package chunk_info

import "sync"

type pendingFinderInfo struct {
	sync.RWMutex
	finder map[string]struct{}
}

func (pfi *pendingFinderInfo) updatePendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	pfi.finder[rootCid] = struct{}{}
}

func (pfi *pendingFinderInfo) cancelPendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(rootCid) == 0 {
		return
	}
	delete(pfi.finder, rootCid)
}

func (pfi *pendingFinderInfo) getPendingFinder(rootCid string) bool {
	pfi.RLock()
	defer pfi.RUnlock()
	_, ok := pfi.finder[rootCid]
	if !ok {
		return false
	}
	return true
}
