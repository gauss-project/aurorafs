package chunk_info

func (pfi *PendingFinderInfo) updatePendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	pfi.finder[rootCid] = struct{}{}
}

func (pfi *PendingFinderInfo) cancelPendingFinder(rootCid string) {
	pfi.Lock()
	defer pfi.Unlock()
	if len(rootCid) == 0 {
		return
	}
	delete(pfi.finder, rootCid)
}

func (pfi *PendingFinderInfo) getPendingFinder(rootCid string) bool {
	pfi.RLock()
	defer pfi.RUnlock()
	_, ok := pfi.finder[rootCid]
	if !ok {
		return false
	}
	return true
}
