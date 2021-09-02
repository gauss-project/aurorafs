package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
	"time"
)

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootcid:overlays:bitvector
	presence map[string]map[string]*bitvector.BitVector
	// rootcid_node : bitvector
	// todo db presence map[string][]byte
	// rootcid:nodes
	overlays map[string][]boson.Address
}

// todo init 从数据库添加数据

func newChunkInfoDiscover() *chunkInfoDiscover {
	return &chunkInfoDiscover{overlays: make(map[string][]boson.Address), presence: make(map[string]map[string]*bitvector.BitVector)}
}

// isExists
func (cd *chunkInfoDiscover) isExists(rootCid boson.Address) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid.String()]
	return ok
}

// getChunkInfo
func (ci *ChunkInfo) getChunkInfo(rootCid, cid boson.Address) [][]byte {
	ci.cd.RLock()
	defer ci.cd.RUnlock()
	res := make([][]byte, 0)
	for overlay, bv := range ci.cd.presence[rootCid.String()] {
		over := boson.MustParseHexAddress(overlay)
		s := ci.cp.getCidStore(rootCid, cid)
		if bv.Get(s) {
			res = append(res, over.Bytes())
		}
	}
	return res
}

// updateChunkInfo
func (cd *chunkInfoDiscover) updateChunkInfo(rootCid, overlay boson.Address, bv []byte) {
	cd.Lock()
	defer cd.Unlock()
	// todo leveDb
	exists := false
	rc := rootCid.String()
	for _, over := range cd.overlays[rc] {
		if over.Equal(overlay) {
			exists = true
			break
		}
	}
	if !exists {
		cd.overlays[rc] = append(cd.overlays[rc], overlay)
		cd.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	vb, ok := cd.presence[rc][overlay.String()]
	if !ok {
		bv, _ := bitvector.NewFromBytes(bv, len(bv)*8)
		cd.presence[rc][overlay.String()] = bv
	} else {
		vb.SetBytes(bv)
	}
}

// createChunkInfoReq
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid boson.Address) pb.ChunkInfoReq {
	ciReq := pb.ChunkInfoReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
	return ciReq
}

// doFindChunkInfo
func (ci *ChunkInfo) doFindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address) {
	ci.queueProcess(ctx, rootCid)
}
