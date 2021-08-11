package chunkinfo

import "sync"

// pendingFinderInfo
type pendingFinderInfo struct {
	sync.RWMutex
	// rootCid
	finder map[string]struct{}
}

// updatePendingFinder
func (pfi *pendingFinderInfo) updatePendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	pfi.finder[rootCid] = struct{}{}
}

// cancelPendingFinder
func (pfi *pendingFinderInfo) cancelPendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(rootCid) == 0 {
		return
	}
	delete(pfi.finder, rootCid)
}

// getPendingFinder
func (pfi *pendingFinderInfo) getPendingFinder(rootCid string) bool {
	pfi.RLock()
	defer pfi.RUnlock()
	_, ok := pfi.finder[rootCid]
	return ok
}
