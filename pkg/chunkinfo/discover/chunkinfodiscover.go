package discover

import (
	send "github.com/ethersphere/bee/pkg/chunkinfo/send"
	"sync"
	"time"
)

type ChunkInfoDiscover struct {
	sync.RWMutex
	presence map[string]map[string][]string
}

type ChunkInfoReq struct {
	rootCid    string
	createTime int64
}

func New() *ChunkInfoDiscover {
	return &ChunkInfoDiscover{presence: make(map[string]map[string][]string)}
}

func (cd *ChunkInfoDiscover) getChunkInfo(rootCid string, cid string) []string {
	cd.RLock()
	defer cd.RUnlock()
	return cd.presence[rootCid][cid]
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

func (cd *ChunkInfoDiscover) DoFindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	// todo 定时器 对请求超时做处理
	ciReq := cd.createChunkInfoReq(rootCid)
	for _, node := range nodes {
		send.SendDataToNode(ciReq, node)
	}
}
