package chunkinfo

import (
	proto "github.com/gogo/protobuf/proto"
	"sync"
)

// chunkInfoTabNeighbor
type chunkInfoTabNeighbor struct {
	sync.RWMutex
	// rootCid:cids or cid:nodes
	presence map[string][]string
}

// chunkInfoResp
type chunkInfoResp struct {
	rootCid  string
	presence map[string][]string
}

func (resp *chunkInfoResp) Reset() {
	*resp = chunkInfoResp{}
}

func (resp *chunkInfoResp) String() string {
	return proto.CompactTextString(resp)
}

func (*chunkInfoResp) ProtoMessage() {
}

// updateNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) updateNeighborChunkInfo(rootCid string, cid string, node string) {
	cn.Lock()
	defer cn.Unlock()
	// todo levelDB
	_, ok := cn.presence[rootCid]
	if !ok {
		cn.presence[rootCid] = make([]string, 0, 1)
	}
	key := rootCid + "_" + cid
	_, pok := cn.presence[key]
	if !pok {
		cn.presence[key] = make([]string, 0, 1)
		cn.presence[rootCid] = append(cn.presence[rootCid], cid)
	}
	cn.presence[key] = append(cn.presence[key], node)
}

// getNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) getNeighborChunkInfo(rootCid string) map[string][]string {
	cn.RLock()
	defer cn.RUnlock()
	res := make(map[string][]string)
	cids := cn.presence[rootCid]
	for _, cid := range cids {
		key := rootCid + "_" + cid
		// todo levelDB
		nodes := cn.presence[key]
		res[cid] = nodes
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid string, ctn map[string][]string) chunkInfoResp {
	return chunkInfoResp{rootCid: rootCid, presence: ctn}
}
