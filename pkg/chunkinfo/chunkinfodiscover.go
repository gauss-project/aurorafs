package chunk_info

import (
	"time"
)

func (cd *ChunkInfoDiscover) isExists(rootCid string) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid]
	return ok
}

func (cd *ChunkInfoDiscover) getChunkInfo(rootCid string, cid string) *[]string {
	cd.RLock()
	defer cd.RUnlock()
	v, _ := cd.presence[rootCid][cid]
	return &v
}

func (cd *ChunkInfoDiscover) UpdateChunkInfo(rootCid string, pyramids map[string][]string) {
	cd.Lock()
	defer cd.RUnlock()
	for k, v := range pyramids {
		cd.updateChunkInfo(rootCid, k, v)
	}
}

func (cd *ChunkInfoDiscover) updateChunkInfo(rootCid string, cid string, nodes []string) {
	// todo 考虑持久化，做数据恢复 nodes 重复问题
	cd.presence[rootCid][cid] = nodes
}

func (cd *ChunkInfoDiscover) createChunkInfoReq(rootCid string) ChunkInfoReq {
	ciReq := ChunkInfoReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return ciReq
}

func (cd *ChunkInfoDiscover) doFindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	// todo 定时器 对请求超时做处理
	// pulled + pulling >= pullMax
	// pull 过程
	ciReq := cd.createChunkInfoReq(rootCid)
	for _, node := range nodes {
		SendDataToNode(ciReq, node)
	}
}
