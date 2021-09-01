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

	// rootcid-node : bitvector
	presence map[string][]byte
	// rootcid:nodes
	overlays map[string][][]byte
}

func newChunkInfoTabNeighbor() *chunkInfoTabNeighbor {
	return &chunkInfoTabNeighbor{overlays: make(map[string][][]byte), presence: make(map[string][]byte)}
}

// todo init 从数据库添加数据

// updateNeighborChunkInfo
func (ci *ChunkInfo) updateNeighborChunkInfo(ctx context.Context, rootCid, cid boson.Address, overlay boson.Address) {
	ci.ct.Lock()
	defer ci.ct.Unlock()
	// todo levelDB
	rc := rootCid.String()
	_, ok := ci.ct.presence[rc]
	if !ok {
		ci.ct.overlays[rc] = make([][]byte, 0, 1)
	}
	key := rc + "_" + overlay.String()
	_, pok := ci.ct.overlays[key]
	if !pok {
		ci.ct.presence[key] = make([]byte, 0, 1)
	}
	cids := ci.getChunkCid(ctx, rootCid)
	// 获取rootCid 所有的cid
	bv, _ := bitvector.New(len(cids))
	bv.SetBytes(ci.ct.presence[key])
	v := ci.cp.getCidStore(rootCid, cid)
	bv.Set(v)
	ci.ct.presence[key] = bv.Bytes()
	ci.ct.overlays[key] = append(ci.ct.overlays[key], overlay.Bytes())
}

func (cn *chunkInfoTabNeighbor) initNeighborChunkInfo(rootCid boson.Address) {
	cn.Lock()
	defer cn.Unlock()
	v := make([][]byte, 0)
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
	nodes := cn.overlays[rc]
	for _, node := range nodes {
		c := boson.NewAddress(node)
		key := rc + "_" + c.String()
		bv := cn.presence[key]
		res[c.String()] = bv
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid boson.Address, ctn map[string][]byte) pb.ChunkInfoResp {
	// todo resp改变 bitvector
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Presence: ctn}
}
