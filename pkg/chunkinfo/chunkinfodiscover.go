package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"strings"
	"sync"
	"time"
)

var discoverKeyPrefix = "discover-"

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootcid:overlays:bitvector
	presence map[string]map[string]*bitvector.BitVector
	// rootcid:nodes
	overlays map[string][]boson.Address
}

func (ci *ChunkInfo) initChunkInfoDiscover() error {
	if err := ci.storer.Iterate(discoverKeyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), discoverKeyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(discoverKeyPrefix, key)
		if err != nil {
			return true, err
		}
		var vb bitVector
		if err := ci.storer.Get(key, &vb); err != nil {
			return true, err
		}
		bit, _ := bitvector.NewFromBytes(vb.B, vb.Len)
		ci.cd.putChunkInfoDiscover(rootCid, overlay, *bit)
		if err := ci.initChunkPyramid(context.Background(), rootCid); err != nil {
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

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

func (cd *chunkInfoDiscover) putChunkInfoDiscover(rootCid, overlay boson.Address, vector bitvector.BitVector) {
	cd.Lock()
	defer cd.Unlock()
	rc := rootCid.String()
	cd.overlays[rc] = append(cd.overlays[rc], overlay)
	if _, ok := cd.presence[rc]; !ok {
		cd.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	cd.presence[rc][overlay.String()] = &vector
}

// updateChunkInfo
func (ci *ChunkInfo) updateChunkInfo(rootCid, overlay boson.Address, bv []byte) {
	ci.cd.Lock()
	defer ci.cd.Unlock()

	rc := rootCid.String()
	over := overlay.String()
	if _, ok := ci.cd.overlays[rc]; !ok {
		ci.cd.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	if _, ok := ci.cd.presence[rootCid.String()][over]; !ok {
		ci.cd.overlays[rc] = append(ci.cd.overlays[rc], overlay)
	}
	vb, ok := ci.cd.presence[rc][overlay.String()]
	if !ok {
		v, _ := ci.getChunkSize(context.Background(), rootCid)
		vb, _ = bitvector.NewFromBytes(bv, v)
		ci.cd.presence[rc][overlay.String()] = vb
	} else {
		if err := vb.SetBytes(bv); err != nil {
			ci.logger.Errorf("chunk discover: set bit vector error")
		}
	}
	// db
	if err := ci.storer.Put(generateKey(discoverKeyPrefix, rootCid, overlay), &bitVector{B: vb.Bytes(), Len: vb.Len()}); err != nil {
		ci.logger.Errorf("chunk discover: put store error")
	}
}

// createChunkInfoReq
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid boson.Address) pb.ChunkInfoReq {
	ciReq := pb.ChunkInfoReq{RootCid: rootCid.Bytes(), CreateTime: time.Now().Unix()}
	return ciReq
}

// doFindChunkInfo
func (ci *ChunkInfo) doFindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address) {
	go ci.queueProcess(ctx, rootCid)
}
