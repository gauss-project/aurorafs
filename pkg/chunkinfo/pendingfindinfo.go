package chunk_info

import "sync"

// pendingFinderInfo 是否发现rootCid
type pendingFinderInfo struct {
	sync.RWMutex
	// rootCid
	finder map[string]struct{}
}

// updatePendingFinder 新增rootCid发现
func (pfi *pendingFinderInfo) updatePendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	pfi.finder[rootCid] = struct{}{}
}

// cancelPendingFinder 根据rootCid取消发现
func (pfi *pendingFinderInfo) cancelPendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(rootCid) == 0 {
		return
	}
	delete(pfi.finder, rootCid)
}

// getPendingFinder 根据rootCid查看是否继续发现
func (pfi *pendingFinderInfo) getPendingFinder(rootCid string) bool {
	pfi.RLock()
	defer pfi.Unlock()
	_, ok := pfi.finder[rootCid]
	if !ok {
		return false
	}
	return true
}
