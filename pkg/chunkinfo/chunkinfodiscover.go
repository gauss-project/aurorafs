package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
	"time"
)

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootCid:cid:overlays
	presence map[string]map[string][][]byte
}

func newChunkInfoDiscover() *chunkInfoDiscover {
	return &chunkInfoDiscover{presence: make(map[string]map[string][][]byte)}
}

// isExists
func (cd *chunkInfoDiscover) isExists(rootCid boson.Address) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid.String()]
	return ok
}

// getChunkInfo
func (cd *chunkInfoDiscover) getChunkInfo(rootCid, cid boson.Address) [][]byte {
	cd.RLock()
	defer cd.RUnlock()
	v, _ := cd.presence[rootCid.String()][cid.String()]
	return v
}

// updateChunkInfos
func (cd *chunkInfoDiscover) updateChunkInfos(rootCid boson.Address, pyramids map[string]*pb.Overlays) {
	cd.Lock()
	defer cd.Unlock()
	for k, v := range pyramids {
		cd.updateChunkInfo(rootCid.String(), k, v.V)
	}
}

// updateChunkInfo
func (cd *chunkInfoDiscover) updateChunkInfo(rootCid, cid string, overlays [][]byte) {
	// todo leveDb
	mn := make(map[string]struct{}, len(overlays))
	for _, n := range overlays {
		mn[boson.NewAddress(n).String()] = struct{}{}
	}
	if cd.presence[rootCid] == nil {
		m := make(map[string][][]byte)
		overlays = make([][]byte, 0, len(mn))
		for n := range mn {
			overlays = append(overlays, []byte(n))
		}
		m[cid] = overlays
		cd.presence[rootCid] = m
	} else {
		for _, n := range cd.presence[rootCid][cid] {
			_, ok := mn[boson.NewAddress(n).String()]
			if ok {
				delete(mn, boson.NewAddress(n).String())
			}
		}
		for k := range mn {
			cd.presence[rootCid][cid] = append(cd.presence[rootCid][cid], []byte(k))
		}
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
