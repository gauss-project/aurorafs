package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
)

// chunkInfoTabNeighbor
type chunkInfoTabNeighbor struct {
	sync.RWMutex
	// rootCid:cids or cid:overlay
	presence map[string][][]byte
}

func newChunkInfoTabNeighbor() *chunkInfoTabNeighbor {
	return &chunkInfoTabNeighbor{presence: make(map[string][][]byte)}
}

// updateNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) updateNeighborChunkInfo(rootCid, cid boson.Address, overlay boson.Address) {
	cn.Lock()
	defer cn.Unlock()
	// todo levelDB
	rc := rootCid.ByteString()
	_, ok := cn.presence[rc]
	if !ok {
		cn.presence[rc] = make([][]byte, 0, 1)
	}
	key := rc + "_" + cid.ByteString()
	_, pok := cn.presence[key]
	if !pok {
		cn.presence[key] = make([][]byte, 0, 1)
		cn.presence[rc] = append(cn.presence[rc], cid.Bytes())
	}
	cn.presence[key] = append(cn.presence[key], overlay.Bytes())
}

// getNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) getNeighborChunkInfo(rootCid boson.Address) map[string]*pb.Overlays {
	cn.RLock()
	defer cn.RUnlock()
	res := make(map[string]*pb.Overlays)
	rc := rootCid.ByteString()
	cids := cn.presence[rc]
	for _, cid := range cids {
		c := string(cid)
		key := rc + "_" + c
		// todo levelDB
		overlays := cn.presence[key]
		res[c] = &pb.Overlays{V: overlays}
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid boson.Address, ctn map[string]*pb.Overlays) pb.ChunkInfoResp {
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Presence: ctn}
}
