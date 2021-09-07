package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"strings"
	"sync"
)

var keyPrefix = "chunk-"

// chunkInfoTabNeighbor
type chunkInfoTabNeighbor struct {
	sync.RWMutex
	// rootCid:node:bitvector
	presence map[string]map[string]*bitvector.BitVector
	// rootCid:node
	overlays map[string][]boson.Address
}

func newChunkInfoTabNeighbor() *chunkInfoTabNeighbor {
	return &chunkInfoTabNeighbor{overlays: make(map[string][]boson.Address), presence: make(map[string]map[string]*bitvector.BitVector)}
}

func (ci *ChunkInfo) initChunkInfoTabNeighbor() error {
	if err := ci.storer.Iterate(keyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), keyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(keyPrefix, key)
		if err != nil {
			return true, err
		}
		var vb bitVector
		if err := ci.storer.Get(key, &vb); err != nil {
			return true, err
		}
		bit, _ := bitvector.NewFromBytes(vb.B, vb.Len)
		ci.ct.putChunkInfoTabNeighbor(rootCid, overlay, *bit)
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func (cn *chunkInfoTabNeighbor) putChunkInfoTabNeighbor(rootCid, overlay boson.Address, vector bitvector.BitVector) {
	cn.Lock()
	defer cn.Unlock()
	rc := rootCid.String()
	cn.overlays[rc] = append(cn.overlays[rc], overlay)
	if _, ok := cn.presence[rc]; !ok {
		cn.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	cn.presence[rc][overlay.String()] = &vector
}

// updateNeighborChunkInfo
func (ci *ChunkInfo) updateNeighborChunkInfo(rootCid, cid boson.Address, overlay boson.Address) error {
	ci.ct.Lock()
	defer ci.ct.Unlock()

	rc := rootCid.String()
	over := overlay.String()
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

	vb, ok := ci.ct.presence[rc][over]
	if !ok {
		v, _ := ci.getChunkSize(context.Background(), rootCid)
		vb, _ = bitvector.New(v)
		ci.ct.presence[rc][over] = vb
	}
	v := ci.cp.getCidStore(rootCid, cid)
	vb.Set(v)
	// db
	return ci.storer.Put(generateKey(keyPrefix, rootCid, overlay), &bitVector{B: vb.Bytes(), Len: vb.Len()})
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
