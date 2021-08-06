package pending

import (
	"sync"
)

type PendingFinderInfo struct {
	sync.RWMutex
	finder map[string]map[string]struct{}
}

func New() *PendingFinderInfo {
	return &PendingFinderInfo{finder: make(map[string]map[string]struct{})}
}

func (pfi *PendingFinderInfo) updatePendingFinder(rootCid string, cids []string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(cids) == 0 {
		return
	}
	for _, cid := range cids {
		pfi.finder[rootCid][cid] = struct{}{}
	}
}

func (pfi *PendingFinderInfo) cancelPendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(rootCid) == 0 {
		return
	}
	delete(pfi.finder, rootCid)
}

func (pfi *PendingFinderInfo) getPendingFinder(rootCid, cid string) bool {
	pfi.RLock()
	defer pfi.RUnlock()
	_, ok := pfi.finder[rootCid]
	if !ok {
		return false
	}
	_, fok := pfi.finder[rootCid][cid]
	if !fok {
		return false
	}
	return true
}
