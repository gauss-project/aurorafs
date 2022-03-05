package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/aurora"
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
	overlays      map[string][]boson.Address
	serverPutChan chan chunkPut
}

func newChunkInfoTabNeighbor() *chunkInfoTabNeighbor {
	return &chunkInfoTabNeighbor{overlays: make(map[string][]boson.Address), presence: make(map[string]map[string]*bitvector.BitVector),
		serverPutChan: make(chan chunkPut, 200)}
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
		var vb BitVector
		if err := ci.storer.Get(key, &vb); err != nil {
			return true, err
		}
		bit, _ := bitvector.NewFromBytes(vb.B, vb.Len)
		ci.ct.putChunkInfoTabNeighbor(rootCid, overlay, *bit)
		if err := ci.initChunkPyramid(context.Background(), rootCid); err != nil {
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func (cn *chunkInfoTabNeighbor) putChunkInfoTabNeighbor(rootCid, overlay boson.Address, vector bitvector.BitVector) {

	rc := rootCid.String()
	cn.overlays[rc] = append(cn.overlays[rc], overlay)
	if _, ok := cn.presence[rc]; !ok {
		cn.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	cn.presence[rc][overlay.String()] = &vector
}

// updateNeighborChunkInfo
func (ci *ChunkInfo) updateNeighborChunkInfo(rootCid, cid boson.Address, overlay, target boson.Address) error {
	rc := rootCid.String()
	over := overlay.String()
	_, ok := ci.ct.presence[rc]
	if !ok {
		return fmt.Errorf("rootCid is not exists")
	}
	if err := ci.putChunkInfoNeighbor(rootCid, overlay); err != nil {
		return err
	}
	bv, ok := ci.ct.presence[rc][over]

	v := ci.getCidSort(rootCid, cid)
	bv.Set(v)
	bit := BitVector{B: bv.Bytes(), Len: bv.Len()}
	if overlay.Equal(ci.addr) {
		ci.Publish(fmt.Sprintf("%s%s", "down", rootCid.String()), BitVectorInfo{
			RootCid:   rootCid,
			Bitvector: bit,
		})
	} else {
		ci.Publish(fmt.Sprintf("%s%s", "retrieval", rootCid.String()), BitVectorInfo{
			RootCid:   rootCid,
			Overlay:   overlay,
			Bitvector: bit,
		})
	}

	// db
	return ci.storer.Put(generateKey(keyPrefix, rootCid, overlay), &bit)
}

func (ci *ChunkInfo) putChunkInfoNeighbor(rootCid, overlay boson.Address) error {

	v, _ := ci.getChunkSize(context.Background(), rootCid)
	if v == 0 {
		return fmt.Errorf("pyramid is not exists")
	}

	_, ok := ci.ct.presence[rootCid.String()][overlay.String()]

	if !ok {
		b, _ := bitvector.New(v)
		ci.ct.putChunkInfoTabNeighbor(rootCid, overlay, *b)
		return ci.storer.Put(generateKey(keyPrefix, rootCid, overlay), &BitVector{B: b.Bytes(), Len: b.Len()})
	}
	return nil
}

func (ci *ChunkInfo) initNeighborChunkInfo(rootCid, peer boson.Address, cids [][]byte) {

	rc := rootCid.String()
	_, ok := ci.ct.presence[rc]

	if !ok {
		err := ci.putChunkInfoNeighbor(rootCid, ci.addr)
		if err != nil {
			ci.logger.Errorf("chunkInfo:putChunkInfoNeighbor error:%v", err)
			return
		}
	}

	for _, cid := range cids {
		c := boson.NewAddress(cid)
		err := ci.UpdateChunkInfoSource(rootCid, peer, c)
		if err != nil {
			ci.logger.Errorf("chunkInfo:UpdateChunkInfoSource error:%v", err)
			return
		}
		err = ci.updateNeighborChunkInfo(rootCid, c, ci.addr, boson.ZeroAddress)
		if err != nil {
			ci.logger.Errorf("chunkInfo:updateNeighborChunkInfo error:%v", err)
			return
		}
	}
}

func (cn *chunkInfoTabNeighbor) isExists(rootCid boson.Address) bool {
	cn.RLock()
	defer cn.RUnlock()
	rc := rootCid.String()
	_, ok := cn.presence[rc]
	return ok
}

func (cn *chunkInfoTabNeighbor) isDownload(rootCid, overlay boson.Address) bool {
	cn.RLock()
	defer cn.RUnlock()
	rc := rootCid.String()
	if v, b := cn.presence[rc]; b {
		return v[overlay.String()].Equals()
	}
	return false
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

func (ci *ChunkInfo) getChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	ci.ct.RLock()
	defer ci.ct.RUnlock()
	res := make([]aurora.ChunkInfoOverlay, 0)
	for overlay, bit := range ci.ct.presence[rootCid.String()] {
		bv := aurora.BitVectorApi{Len: bit.Len(), B: bit.Bytes()}
		cio := aurora.ChunkInfoOverlay{Overlay: overlay, Bit: bv}
		res = append(res, cio)
	}
	return res
}

// createChunkInfoResp
func (cn *chunkInfoTabNeighbor) createChunkInfoResp(rootCid boson.Address, ctn map[string][]byte, target, req []byte) pb.ChunkInfoResp {
	return pb.ChunkInfoResp{RootCid: rootCid.Bytes(), Target: target, Req: req, Presence: ctn}
}

func (ci *ChunkInfo) delPresence(rootCid boson.Address) bool {

	delKey := fmt.Sprintf("%s%s", keyPrefix, rootCid.String())
	if err := ci.storer.Iterate(delKey, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), delKey) {
			return false, nil
		}
		key := string(k)
		err := ci.storer.Delete(key)
		if err != nil {
			return true, fmt.Errorf("del rootCid: %s : neighbor %v", rootCid.String(), err)
		}
		return false, nil
	}); err != nil {
		ci.logger.Errorf("chunkinfo : %v", err)
		return false
	}

	delete(ci.ct.presence, rootCid.String())
	delete(ci.ct.overlays, rootCid.String())

	return true
}

func (cn *chunkInfoTabNeighbor) setLock() {
	cn.Lock()
}
func (cn *chunkInfoTabNeighbor) setUnLock() {
	cn.Unlock()
}
func (cn *chunkInfoTabNeighbor) getChan() chan chunkPut {
	return cn.serverPutChan
}
