package pyramid

import (
	send "github.com/ethersphere/bee/pkg/chunkinfo/send"
	"sync"
	"time"
)

type ChunkPyramid struct {
	sync.RWMutex
	// 切片父id/切片id/切片所在树节点顺序
	Pyramid map[string]map[string]uint
}

type ChunkPyramidReq struct {
	rootCid    string
	createTime int64
}

// todo 验证金字塔结构是否正确

func New() *ChunkPyramid {
	return &ChunkPyramid{Pyramid: map[string]map[string]uint{}}
}

func (cp *ChunkPyramid) UpdateChunkPyramid(pyramids map[string]map[string]uint) {
	cp.Lock()
	defer cp.RUnlock()
	for key, pyramid := range pyramids {
		cp.Pyramid[key] = pyramid
	}
}

func (cp *ChunkPyramid) getChunkPyramid(rootCid string) map[*string]*map[string]uint {
	cp.RLock()
	defer cp.RUnlock()
	ps := make(map[*string]*map[string]uint)
	cp.getChunkPyramidByPCid(rootCid, ps)
	return ps
}

func (cp *ChunkPyramid) getChunkPyramidByPCid(pCid string, pyramids map[*string]*map[string]uint) {
	cids, ok := cp.Pyramid[pCid]
	if !ok {
		return
	}
	pyramids[&pCid] = &cids
	for cid, _ := range cids {
		cp.getChunkPyramidByPCid(cid, pyramids)
	}
}

func (cp *ChunkPyramid) createChunkPyramidReq(rootCid string) ChunkPyramidReq {
	cpReq := ChunkPyramidReq{rootCid: rootCid, createTime: time.Now().Unix()}
	return cpReq
}

func (cp *ChunkPyramid) DoFindChunkPyramid(authInfo []byte, rootCid string, nodes []string) {
	// 调用sendDataToNodes
	cpReq := cp.createChunkPyramidReq(rootCid)
	for _, node := range nodes {
		send.SendDataToNode(cpReq, node)
	}
}
