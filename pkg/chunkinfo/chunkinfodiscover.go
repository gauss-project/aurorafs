package chunk_info

import (
	"sync"
	"time"
)

// chunkInfoDiscover 发现chunk
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootCid-> cid -> nodes
	presence map[string]map[string][]string
}

// chunkInfoReq 请求体
type chunkInfoReq struct {
	rootCid    string
	createTime int64
}

// isExists 判断rootCid是否存在
func (cd *chunkInfoDiscover) isExists(rootCid string) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid]
	return ok
}

// getChunkInfo 根据rootCid与cid获取nodes
func (cd *chunkInfoDiscover) getChunkInfo(rootCid string, cid string) []string {
	cd.RLock()
	defer cd.RUnlock()
	v, _ := cd.presence[rootCid][cid]
	return v
}

// updateChunkInfos 根据rootCid新增 cid对应节点
func (cd *chunkInfoDiscover) updateChunkInfos(rootCid string, pyramids map[string][]string) {
	cd.Lock()
	defer cd.Unlock()
	for k, v := range pyramids {
		cd.updateChunkInfo(rootCid, k, v)
	}
}

// updateChunkInfo  根据rootCid与cid新增nodes
func (cd *chunkInfoDiscover) updateChunkInfo(rootCid string, cid string, nodes []string) {
	// todo 考虑持久化，做数据恢复
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

// createChunkInfoReq 创建chunkInfo请求
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid string) chunkInfoReq {
	ciReq := chunkInfoReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return ciReq
}

// doFindChunkInfo 发现过程
func (ci *ChunkInfo) doFindChunkInfo(authInfo []byte, rootCid string) {
	// pull 过程
	ci.queueProcess(rootCid)

}
