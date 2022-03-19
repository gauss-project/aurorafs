package chunkinfo

import (
	"context"
	"fmt"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
)

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	hashData       map[string]chunkInfo
	chunk          map[string]uint
	pyramidPutChan chan chunkPut
}

type chunkInfo struct {
	hashMax  uint
	chunkMax uint
}

type pyramid struct {
	cids map[string]pyramidCid
}

type pyramidCid struct {
	sort   int
	number int
}

type PyramidCidNum struct {
	Cid    boson.Address
	Number int
}

func newChunkPyramid() *chunkPyramid {
	chunkPayramid := &chunkPyramid{
		hashData:       make(map[string]chunkInfo),
		chunk:          make(map[string]uint),
		pyramidPutChan: make(chan chunkPut, 1000)}
	return chunkPayramid
}

func (ci *ChunkInfo) initChunkPyramid(ctx context.Context, rootCid boson.Address) error {
	if _, ok := ci.cp.hashData[rootCid.String()]; ok {
		return nil
	}
	trie, err := ci.traversal.GetPyramid(ctx, rootCid)
	if err != nil {
		return err
	}
	data, _, err := ci.traversal.GetChunkHashes(ctx, rootCid, nil)
	if err != nil {
		return err
	}
	ci.updateChunkPyramid(rootCid, data, trie)
	return nil
}

// updateChunkPyramid
func (ci *ChunkInfo) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte, trie map[string][]byte) [][]byte {
	cids := make([][]byte, 0)
	py := make(map[string]struct{}, len(pyramids))
	var hashMax, chunkMax uint
	for _, p := range pyramids {
		for _, x := range p {
			cid := boson.NewAddress(x).String()
			if _, ok := py[cid]; !ok {
				if ci.cp.putChunk(boson.NewAddress(x)) {
					cids = append(cids, x)
				}
			}
			chunkMax++
		}
	}
	for k := range trie {
		if _, ok := py[k]; !ok {
			hashMax++
			ci.cp.putChunk(boson.MustParseHexAddress(k))
		}

	}
	ci.cp.hashData[rootCid.String()] = chunkInfo{
		chunkMax: chunkMax,
		hashMax:  hashMax,
	}
	return cids
}

func (ci *ChunkInfo) getPyramid(rootCid boson.Address) (*pyramid, error) {
	key := fmt.Sprintf("pyramid-%s", rootCid.String())
	v, _, err := ci.singleflight.Do(context.Background(), key, func(ctx context.Context) (interface{}, error) {
		py := make(map[string]pyramidCid)
		pyramids, _, err := ci.traversal.GetChunkHashes(context.Background(), rootCid, nil)
		if err != nil {
			return nil, err
		}
		var sort int
		for _, p := range pyramids {
			for _, x := range p {
				if v, ok := py[boson.NewAddress(x).String()]; !ok {
					py[boson.NewAddress(x).String()] = pyramidCid{
						sort:   sort,
						number: 1,
					}
					sort++
				} else {
					v.number = v.number + 1
					py[boson.NewAddress(x).String()] = v
				}
			}
		}
		return &pyramid{
			cids: py,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*pyramid), nil
}

func (ci *ChunkInfo) getPyramidHash(rootCid boson.Address) (*[]string, error) {
	mate, err := ci.getChunkPyramid(context.Background(), rootCid)
	if err != nil {
		return nil, err
	}
	pyramid, err := ci.getPyramid(rootCid)
	if err != nil {
		return nil, err
	}
	hash := make([]string, 0)
	for k := range mate {
		if _, ok := pyramid.cids[k]; !ok {
			hash = append(hash, k)
		}
	}
	return &hash, nil
}

func (cp *chunkPyramid) putChunk(cid boson.Address) bool {
	if v, ok := cp.chunk[cid.String()]; ok {
		cp.chunk[cid.String()] = v + 1
		return true
	} else {
		cp.chunk[cid.String()] = 1
	}
	return false
}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	key := fmt.Sprintf("mate-%s", rootCid.String())
	mate, _, err := ci.singleflight.Do(cxt, key, func(ctx context.Context) (interface{}, error) {
		v, err := ci.traversal.GetPyramid(cxt, rootCid)
		if err != nil {
			return nil, err
		}
		return v, nil
	})
	if err != nil {
		return nil, err
	}
	return mate.(map[string][]byte), nil
}

func (ci *ChunkInfo) isExists(rootCid boson.Address) bool {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	_, ok := ci.cp.hashData[rootCid.String()]
	return ok
}

func (ci *ChunkInfo) getChunkSize(cxt context.Context, rootCid boson.Address) (int, error) {
	info, ok := ci.cp.hashData[rootCid.String()]
	if !ok {
		v, err := ci.getChunkPyramid(context.Background(), rootCid)
		if err != nil {
			return 0, nil
		}
		trie, _, err := ci.traversal.GetChunkHashes(cxt, rootCid, nil)
		if err != nil {
			return 0, err
		}
		ci.updateChunkPyramid(rootCid, trie, v)
		return ci.getChunkSize(cxt, rootCid)
	}
	return int(info.chunkMax), nil
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlay boson.Address) error {
	if ci.isExists(rootCid) {
		return nil
	}
	req := pb.ChunkPyramidReq{
		RootCid: rootCid.Bytes(),
		Target:  overlay.Bytes(),
	}
	return ci.sendPyramids(ctx, overlay, streamPyramidName, req)
}

func (ci *ChunkInfo) getUnRepeatChunk(rootCid boson.Address) []*PyramidCidNum {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	v, err := ci.getPyramid(rootCid)
	if err != nil {
		return nil
	}
	mate, err := ci.getChunkPyramid(context.Background(), rootCid)
	if err != nil {
		return nil
	}
	cids := make([]*PyramidCidNum, 0, len(v.cids)+len(mate))
	for overlay, c := range v.cids {
		v := ci.cp.chunk[overlay]
		if v > 1 {
			continue
		}
		over := boson.MustParseHexAddress(overlay)
		pcn := PyramidCidNum{Cid: over, Number: c.number}
		cids = append(cids, &pcn)
	}

	for overlay := range mate {
		c := ci.cp.chunk[overlay]
		if c > 1 {
			continue
		}
		if _, ok := v.cids[overlay]; !ok {
			over := boson.MustParseHexAddress(overlay)
			pcn := PyramidCidNum{Cid: over, Number: 1}
			cids = append(cids, &pcn)
		}
	}
	return cids
}

func (ci *ChunkInfo) getChunkCid(rootCid boson.Address) []*PyramidCidNum {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	v, err := ci.getPyramid(rootCid)
	if err != nil {
		return nil
	}
	cids := make([]*PyramidCidNum, 0, len(v.cids))
	for overlay, c := range v.cids {
		over := boson.MustParseHexAddress(overlay)
		pcn := PyramidCidNum{Cid: over, Number: c.number}
		cids = append(cids, &pcn)
	}
	return cids
}

func (ci *ChunkInfo) getCidSort(rootCid, cid boson.Address) int {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	pyramid, err := ci.getPyramid(rootCid)
	if err != nil {
		return 0
	}
	return pyramid.cids[cid.String()].sort
}

// func (cp *chunkPyramid) updateCidSort(rootCid, cid boson.Address, sort int) {
//
//	v, ok := cp.pyramid[rootCid.String()][cid.String()]
//	if !ok {
//		return
//	}
//	v.sort = sort
//	cp.pyramid[rootCid.String()][cid.String()] = v
// }

func (ci *ChunkInfo) getRootChunk(rootCid string) int {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	info, ok := ci.cp.hashData[rootCid]
	if !ok {
		return 0
	}
	return int(info.chunkMax)
}

func (cp *chunkPyramid) getRootHash(rootCID string) int {
	cp.RLock()
	defer cp.RUnlock()
	info, ok := cp.hashData[rootCID]
	if !ok {
		return 0
	}
	return int(info.hashMax)
}

func (cp *chunkPyramid) delChunk(cid boson.Address) {
	v := cp.chunk[cid.String()]
	if v > 1 {
		cp.chunk[cid.String()] = v - 1
	} else {
		delete(cp.chunk, cid.String())
	}
}

func (ci *ChunkInfo) delRootCid(rootCID boson.Address, pyr pyramid, hashs []string) bool {
	for _, cid := range hashs {
		ci.cp.delChunk(boson.MustParseHexAddress(cid))
	}
	for cid := range pyr.cids {
		ci.cp.delChunk(boson.MustParseHexAddress(cid))
	}
	delete(ci.cp.hashData, rootCID.String())
	return true
}

func (cp *chunkPyramid) setLock() {
	cp.Lock()
}
func (cp *chunkPyramid) setUnLock() {
	cp.Unlock()
}
func (cp *chunkPyramid) getChan() chan chunkPut {
	return cp.pyramidPutChan
}
