package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"sync"
)

// pendingFinderInfo
type pendingFinderInfo struct {
	sync.RWMutex
	// rootCid
	finder map[string]struct{}
}

func newPendingFinderInfo() *pendingFinderInfo {
	return &pendingFinderInfo{finder: make(map[string]struct{})}
}

// updatePendingFinder
func (pfi *pendingFinderInfo) updatePendingFinder(rootCid boson.Address) {
	pfi.Lock()
	defer pfi.Unlock()
	pfi.finder[rootCid.String()] = struct{}{}
}

// cancelPendingFinder
func (pfi *pendingFinderInfo) cancelPendingFinder(rootCid boson.Address) {
	pfi.Lock()
	defer pfi.Unlock()
	delete(pfi.finder, rootCid.String())
}

// getPendingFinder
func (pfi *pendingFinderInfo) getPendingFinder(rootCid boson.Address) bool {
	pfi.RLock()
	defer pfi.RUnlock()
	_, ok := pfi.finder[rootCid.String()]
	return ok
}
