package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
)

// chunkInfoTabNeighbor
type chunkInfoTabNeighbor struct {
	sync.RWMutex

	presence map[string]map[string]*bitvector.BitVector
	// rootcid-node : bitvector
	//todo db presence map[string][]byte
	// rootcid:nodes
	overlays map[string][]boson.Address
}

func newChunkInfoTabNeighbor() *chunkInfoTabNeighbor {
	return &chunkInfoTabNeighbor{overlays: make(map[string][]boson.Address), presence: make(map[string]map[string]*bitvector.BitVector)}
}

// todo init 从数据库添加数据

// updateNeighborChunkInfo
func (ci *ChunkInfo) updateNeighborChunkInfo(rootCid, cid boson.Address, overlay boson.Address) {
	ci.ct.Lock()
	defer ci.ct.Unlock()
	// todo levelDB
	rc := rootCid.String()
	exists := false
	for _, over := range ci.ct.overlays[rootCid.String()] {
		if over.Equal(overlay) {
			exists = true
			break
		}
	}
	if !exists {
		ci.ct.overlays[rc] = append(ci.ct.overlays[rc], overlay)
		ci.ct.presence[rc] = make(map[string]*bitvector.BitVector)
	}

	vb, ok := ci.ct.presence[rc][overlay.String()]
	if !ok {
		v, _ := ci.getChunkPyramidHash(context.Background(), rootCid)
		vb, _ = bitvector.New(len(v))
		ci.ct.presence[rc][overlay.String()] = vb
	}
	v := ci.cp.getCidStore(rootCid, cid)
	vb.Set(v)
}

func (cn *chunkInfoTabNeighbor) initNeighborChunkInfo(rootCid boson.Address) {
	cn.Lock()
	defer cn.Unlock()
	v := make([]boson.Address, 0)
	cn.overlays[rootCid.String()] = v
}

func (cn *chunkInfoTabNeighbor) isExists(rootCid boson.Address) bool {
	cn.RLock()
	defer cn.RUnlock()
	rc := rootCid.String()
	_, b := cn.overlays[rc]
	return b
}

// getNeighborChunkInfo
func (cn *chunkInfoTabNeighbor) getNeighborChunkInfo(rootCid boson.Address) map[string][]byte {
	cn.RLock()
	defer cn.RUnlock()
	res := make(map[string][]byte)
	rc := rootCid.String()
	overlays := cn.overlays[rc]
	for _, overlay := range overlays {
		bv := cn.presence[rc][overlay.String()]
		res[overlay.String()] = bv.Bytes()
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid boson.Address, ctn map[string][]byte) pb.ChunkInfoResp {
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Presence: ctn}
}
