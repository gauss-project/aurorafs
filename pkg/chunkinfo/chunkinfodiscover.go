package chunk_info

import (
	"sync"
	"time"
)

type chunkInfoDiscover struct {
	sync.RWMutex
	presence map[string]map[string][]string
}

type chunkInfoReq struct {
	rootCid    string
	createTime int64
}

func (cd *chunkInfoDiscover) isExists(rootCid string) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid]
	return ok
}

func (cd *chunkInfoDiscover) getChunkInfo(rootCid string, cid string) *[]string {
	cd.RLock()
	defer cd.RUnlock()
	v, _ := cd.presence[rootCid][cid]
	return &v
}

func (cd *chunkInfoDiscover) updateChunkInfos(rootCid string, pyramids map[string][]string) {
	cd.Lock()
	defer cd.RUnlock()
	for k, v := range pyramids {
		cd.updateChunkInfo(rootCid, k, v)
	}
}

func (cd *chunkInfoDiscover) updateChunkInfo(rootCid string, cid string, nodes []string) {
	// todo 考虑持久化，做数据恢复 nodes 重复问题
	cd.presence[rootCid][cid] = append(cd.presence[rootCid][cid], nodes...)
}

func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid string) chunkInfoReq {
	ciReq := chunkInfoReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return ciReq
}
