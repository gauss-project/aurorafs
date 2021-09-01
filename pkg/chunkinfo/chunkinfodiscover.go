package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
	"time"
)

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootcid_node : bitvector
	presence map[string][]byte
	// rootcid:nodes
	overlays map[string][][]byte
}

// todo init 从数据库添加数据

func newChunkInfoDiscover() *chunkInfoDiscover {
	return &chunkInfoDiscover{overlays: make(map[string][][]byte), presence: make(map[string][]byte)}
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
	// todo 根据位图获取nodes
	overlays := ci.cd.overlays[rootCid.String()]
	v := ci.getChunkCid(nil, rootCid)
	res := make([][]byte, 0)
	for _, overlay := range overlays {
		over := boson.NewAddress(overlay)
		key := fmt.Sprintf("%s_%s", rootCid, over.String())
		bv := ci.cd.presence[key]
		b, _ := bitvector.NewFromBytes(bv, len(v))
		s := ci.cp.getCidStore(rootCid, cid)
		if b.Get(s) {
			res = append(res, over.Bytes())
		}
	}
	return res
}

// updateChunkInfos
func (cd *chunkInfoDiscover) updateChunkInfos(rootCid, overlay boson.Address, bv []byte) {
	cd.Lock()
	defer cd.Unlock()
	cd.updateChunkInfo(rootCid, overlay, bv)
}

// updateChunkInfo
func (cd *chunkInfoDiscover) updateChunkInfo(rootCid, overlay boson.Address, bv []byte) {
	// todo leveDb
	// todo 位图结构存储
	exists := false
	for _, over := range cd.overlays[rootCid.String()] {
		if boson.NewAddress(over).String() == overlay.String() {
			exists = true
			break
		}
	}
	if !exists {
		cd.overlays[rootCid.String()] = append(cd.overlays[rootCid.String()], overlay.Bytes())
	}

	key := fmt.Sprintf("%s_%s", rootCid, overlay)
	vb, ok := cd.presence[key]
	if !ok {
		cd.presence[key] = vb
	} else {
		bv, _ := bitvector.NewFromBytes(vb, len(vb)*8)
		bv.SetBytes(vb)
	}

}

// createChunkInfoReq
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid boson.Address) pb.ChunkInfoReq {
	ciReq := pb.ChunkInfoReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
	return ciReq
}

// doFindChunkInfo
func (ci *ChunkInfo) doFindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address) {
	ci.queueProcess(ctx, rootCid, streamChunkInfoReqName)
}
