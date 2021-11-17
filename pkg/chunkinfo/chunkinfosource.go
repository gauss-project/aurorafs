package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"strings"
	"sync"
)

type sourceInfo struct {
	//Pyramid source overlay
	PyramidSource string
	//Chunk obtained by each overlay
	ChunkSource map[string]*bitvector.BitVector
}

type chunkInfoSource struct {
	sync.RWMutex
	// rootCid:overlays:bitvector
	storer   storage.StateStorer
	logger   logging.Logger
	presence map[string]*sourceInfo
}

var chunkSourceKeyPrefix = "sourceChunk-"
var pyramidKeyPrefix = "sourcePyramid-"

func newChunkSource(store storage.StateStorer, logger logging.Logger) *chunkInfoSource {
	return &chunkInfoSource{
		storer:   store,
		logger:   logger,
		presence: make(map[string]*sourceInfo)}
}

func (cs *chunkInfoSource) initChunkInfoSource() error {

	if err := cs.storer.Iterate(pyramidKeyPrefix, func(k, value []byte) (stop bool, err error) {
		if !strings.HasPrefix(string(k), pyramidKeyPrefix) {
			return true, nil
		}

		key := string(k)
		rootCid, _, err := unmarshalKey(pyramidKeyPrefix, key)
		if err != nil {
			return false, err
		}

		var overlay string
		if err = cs.storer.Get(key, &overlay); err != nil {
			return false, err
		}

		cs.putPyramidSource(rootCid, overlay)
		return false, nil
	}); err != nil {
		return err
	}

	if err := cs.storer.Iterate(chunkSourceKeyPrefix, func(k, value []byte) (stop bool, err error) {
		if !strings.HasPrefix(string(k), chunkSourceKeyPrefix) {
			return true, nil
		}

		key := string(k)
		rootCid, overlay, err := unmarshalKey(chunkSourceKeyPrefix, key)
		if err != nil {
			return false, err
		}

		var vb bitVector
		if err = cs.storer.Get(key, &vb); err != nil {
			return false, err
		}
		bit, err := bitvector.NewFromBytes(vb.B, vb.Len)
		if err != nil {
			return false, err
		}
		cs.putChunkInfoChunkInfoSource(rootCid, overlay, *bit)
		return false, nil
	}); err != nil {
		return err
	}

	return nil
}

func (cs *chunkInfoSource) putChunkInfoChunkInfoSource(rootCid, sourceOverlay boson.Address, vector bitvector.BitVector) {
	cs.Lock()
	defer cs.Unlock()
	rc := rootCid.String()
	over := sourceOverlay.String()
	cs.presence[rc].ChunkSource[over] = &vector
}

func (cs *chunkInfoSource) putPyramidSource(rootCid boson.Address, sourceOverlay string) {
	cs.Lock()
	defer cs.Unlock()
	rc := rootCid.String()
	if _, ok := cs.presence[rc]; !ok {
		cs.presence[rc] = &sourceInfo{
			PyramidSource: sourceOverlay,
			ChunkSource:   make(map[string]*bitvector.BitVector),
		}
	}
}

func (ci *ChunkInfo) UpdateChunkInfoSource(rootCid, sourceOverlay boson.Address, cid boson.Address) error {
	ci.cs.Lock()
	defer ci.cs.Unlock()
	rc := rootCid.String()
	over := sourceOverlay.String()
	if _, ok := ci.cs.presence[rc]; !ok {
		ci.cs.presence[rc] = &sourceInfo{
			PyramidSource: "",
			ChunkSource:   make(map[string]*bitvector.BitVector),
		}
	}
	vb, ok := ci.cs.presence[rc].ChunkSource[over]
	if !ok {
		v, _ := ci.getChunkSize(context.Background(), rootCid)
		vb, _ = bitvector.New(v)
		ci.cs.presence[rc].ChunkSource[over] = vb
	}
	v := ci.cp.getCidStore(rootCid, cid)
	vb.Set(v)
	// db
	return ci.cs.storer.Put(generateKey(chunkSourceKeyPrefix, rootCid, sourceOverlay), &bitVector{B: vb.Bytes(), Len: vb.Len()})

}

func (ci *ChunkInfo) UpdatePyramidSource(ctx context.Context, rootCid, sourceOverlay boson.Address) error {
	size, err := ci.getChunkSize(ctx, rootCid)
	if err != nil {
		return err
	}
	err = ci.cs.updatePyramidSource(ctx, rootCid, sourceOverlay)
	if err != nil {
		return err
	}

	if size == 1 {
		cids := ci.cp.getChunkCid(rootCid)
		if len(cids) == 0 {
			ci.logger.Errorf("chunk source: The current cid should not be 0 ")
			return fmt.Errorf("chunk source: The current cid should not be 0 ")
		}
		cid := cids[0].Cid
		err = ci.UpdateChunkInfoSource(rootCid, sourceOverlay, cid)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cs *chunkInfoSource) updatePyramidSource(ctx context.Context, rootCid, sourceOverlay boson.Address) error {
	cs.Lock()
	defer cs.Unlock()
	rc := rootCid.String()
	overlay := sourceOverlay.String()
	if _, ok := cs.presence[rc]; !ok {
		cs.presence[rc] = &sourceInfo{
			PyramidSource: overlay,
			ChunkSource:   make(map[string]*bitvector.BitVector),
		}
	} else {
		cs.presence[rc].PyramidSource = overlay
	}

	return cs.storer.Put(generateKey(pyramidKeyPrefix, rootCid, sourceOverlay), overlay)
}

func (cs *chunkInfoSource) GetChunkInfoSource(rootCid boson.Address) (sourceResp aurora.ChunkInfoSourceApi) {
	cs.RLock()
	defer cs.RUnlock()
	rc := rootCid.String()
	if _, ok := cs.presence[rc]; !ok {
		return
	}
	if len(cs.presence[rc].ChunkSource) == 0 {
		return
	}

	sourceResp.PyramidSource = cs.presence[rc].PyramidSource
	for k, v := range cs.presence[rc].ChunkSource {
		chunkBit := aurora.BitVectorApi{
			Len: v.Len(),
			B:   v.Bytes(),
		}
		source := aurora.ChunkSourceApi{
			Overlay:  k,
			ChunkBit: chunkBit,
		}
		sourceResp.ChunkSource = append(sourceResp.ChunkSource, source)
	}

	return sourceResp
}

func (cs *chunkInfoSource) DelChunkInfoSource(rootCid boson.Address) bool {
	cs.Lock()
	defer cs.Unlock()
	key := fmt.Sprintf("%s%s", chunkSourceKeyPrefix, rootCid.String())
	if err := cs.storer.Iterate(key, func(k, value []byte) (stop bool, err error) {
		key := string(k)
		if !strings.HasPrefix(key, chunkSourceKeyPrefix) {
			return true, nil
		}
		err = cs.storer.Delete(key)
		if err != nil {
			return true, nil
		}
		return false, nil
	}); err != nil {
		cs.logger.Errorf("chunk source: del rootCid chunk source error:%w", err)
		return false
	}
	key = fmt.Sprintf("%s%s", pyramidKeyPrefix, rootCid.String())
	if err := cs.storer.Iterate(key, func(k, value []byte) (stop bool, err error) {
		key := string(k)
		if !strings.HasPrefix(key, pyramidKeyPrefix) {
			return true, nil
		}
		err = cs.storer.Delete(key)
		if err != nil {
			return true, nil
		}
		return false, nil
	}); err != nil {
		cs.logger.Errorf("chunk source: del rootCid pyramid source error:%w", err)
		return false
	}

	delete(cs.presence, rootCid.String())

	return true
}
