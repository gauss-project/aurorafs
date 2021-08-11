package chunkinfo

import (
	"sync"
	"time"
)

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootCid:cid:nodes
	presence map[string]map[string][]string
}

// chunkInfoReq
type chunkInfoReq struct {
	rootCid    string
	createTime int64
}

// isExists
func (cd *chunkInfoDiscover) isExists(rootCid string) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid]
	return ok
}

// getChunkInfo
func (cd *chunkInfoDiscover) getChunkInfo(rootCid string, cid string) []string {
	cd.RLock()
	defer cd.RUnlock()
	v, _ := cd.presence[rootCid][cid]
	return v
}

// updateChunkInfos
func (cd *chunkInfoDiscover) updateChunkInfos(rootCid string, pyramids map[string][]string) {
	cd.Lock()
	defer cd.Unlock()
	for k, v := range pyramids {
		cd.updateChunkInfo(rootCid, k, v)
	}
}

// updateChunkInfo
func (cd *chunkInfoDiscover) updateChunkInfo(rootCid string, cid string, nodes []string) {
	// todo leveDb
	mn := make(map[string]struct{}, len(nodes))
	// 去重
	for _, n := range nodes {
		mn[n] = struct{}{}
	}
	if cd.presence[rootCid] == nil {
		m := make(map[string][]string)
		nodes = make([]string, 0, len(mn))
		for n, _ := range mn {
			nodes = append(nodes, n)
		}
		m[cid] = nodes
		cd.presence[rootCid] = m
	} else {
		for _, n := range cd.presence[rootCid][cid] {
			_, ok := mn[n]
			if ok {
				delete(mn, n)
			}
		}
		for k, _ := range mn {
			cd.presence[rootCid][cid] = append(cd.presence[rootCid][cid], k)
		}
	}
}

// createChunkInfoReq
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid string) chunkInfoReq {
	ciReq := chunkInfoReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return ciReq
}

// doFindChunkInfo
func (ci *ChunkInfo) doFindChunkInfo(authInfo []byte, rootCid string) {
	ci.queueProcess(rootCid)
}
