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
	rc := rootCid.String()
	_, ok := cn.presence[rc]
	if !ok {
		cn.presence[rc] = make([][]byte, 0, 1)
	}
	key := rc + "_" + cid.String()
	_, pok := cn.presence[key]
	if !pok {
		cn.presence[key] = make([][]byte, 0, 1)
		cn.presence[rc] = append(cn.presence[rc], cid.Bytes())
	}
	cn.presence[key] = append(cn.presence[key], overlay.Bytes())
}

func (cn *chunkInfoTabNeighbor) initNeighborChunkInfo(rootCid boson.Address) {
	cn.Lock()
	defer cn.Unlock()
	v := make([][]byte, 0)
	cn.presence[rootCid.String()] = v
}

func (cn *chunkInfoTabNeighbor) isExists(rootCid boson.Address) bool {
	cn.RLock()
	defer cn.RUnlock()
	rc := rootCid.String()
	_, b := cn.presence[rc]
	return b
}

// getNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) getNeighborChunkInfo(rootCid boson.Address) map[string]*pb.Overlays {
	cn.RLock()
	defer cn.RUnlock()
	res := make(map[string]*pb.Overlays)
	rc := rootCid.String()
	cids := cn.presence[rc]
	for _, cid := range cids {
		c := boson.NewAddress(cid)
		key := rc + "_" + c.String()
		// todo levelDB
		overlays := cn.presence[key]
		res[c.String()] = &pb.Overlays{V: overlays}
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid boson.Address, ctn map[string]*pb.Overlays) pb.ChunkInfoResp {
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Presence: ctn}
}
