package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"sync"
)

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	// rootCid:cid:bit len/count
	pyramid map[string]map[string]pyramidCidCount
	// rootCid: hash : data
	mateData map[string]map[string][]byte
	// rootCid: count
	hashData map[string][]string

	chunk map[string]uint
}

type pyramidCidCount struct {
	sort   int
	count  *int
	number int
}

type PyramidCidNum struct {
	Cid    boson.Address
	Number int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]pyramidCidCount), mateData: make(map[string]map[string][]byte), hashData: make(map[string][]string), chunk: make(map[string]uint)}
}

func (ci *ChunkInfo) initChunkPyramid(ctx context.Context, rootCid boson.Address) error {
	if ci.cp.pyramid[rootCid.String()] != nil {
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

//func (cp *chunkPyramid) checkPyramid(rootCid, cid boson.Address) bool {
//	cp.RLock()
//	defer cp.RUnlock()
//	if cp.pyramid[rootCid.String()] != nil {
//		_, ok := cp.pyramid[rootCid.String()][cid.String()]
//		return ok
//	}
//	return false
//}

// updateChunkPyramid
func (ci *ChunkInfo) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte, trie map[string][]byte) {
	ci.cp.Lock()
	defer ci.cp.Unlock()
	py := make(map[string]pyramidCidCount)
	var i, max int
	for _, p := range pyramids {
		for _, x := range p {
			if v, ok := py[boson.NewAddress(x).String()]; !ok {
				py[boson.NewAddress(x).String()] = pyramidCidCount{
					sort:   i,
					count:  &max,
					number: 1,
				}
				ci.cp.putChunk(boson.NewAddress(x))
				i++
			} else {
				v.number = v.number + 1
				py[boson.NewAddress(x).String()] = v
			}
			max++
		}
	}
	hash := make([]string, 0)
	for k := range trie {
		if _, ok := py[k]; !ok {
			hash = append(hash, k)
			ci.cp.putChunk(boson.MustParseHexAddress(k))
		}
	}
	ci.cp.pyramid[rootCid.String()] = py
	ci.cp.hashData[rootCid.String()] = hash
	ci.cp.mateData[rootCid.String()] = trie
}

func (cp *chunkPyramid) putChunk(cid boson.Address) {
	if v, ok := cp.chunk[cid.String()]; ok {
		cp.chunk[cid.String()] = v + 1
	} else {
		cp.chunk[cid.String()] = 1
	}
}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	if v, ok := ci.cp.mateData[rootCid.String()]; ok {
		return v, nil
	}
	v, err := ci.traversal.GetPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cp *chunkPyramid) isExists(rootCid boson.Address) bool {
	cp.RLock()
	defer cp.RUnlock()
	_, ok := cp.pyramid[rootCid.String()]
	return ok
}

func (ci *ChunkInfo) getChunkSize(cxt context.Context, rootCid boson.Address) (int, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return 0, err
	}

	if ci.cp.pyramid[rootCid.String()] == nil {
		trie, _, err := ci.traversal.GetChunkHashes(cxt, rootCid, nil)
		if err != nil {
			return 0, err
		}
		ci.updateChunkPyramid(rootCid, trie, v)
	}

	return len(ci.cp.pyramid[rootCid.String()]), nil
}

func (ci *ChunkInfo) getChunkPyramidHash(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlay boson.Address) error {
	if ci.cp.isExists(rootCid) {
		return nil
	}
	req := pb.ChunkPyramidReq{
		RootCid: rootCid.Bytes(),
		Target:  overlay.Bytes(),
	}
	return ci.sendPyramids(ctx, overlay, streamPyramidName, req)
}

func (cp *chunkPyramid) getChunkCid(rootCid boson.Address) []*PyramidCidNum {
	cp.RLock()
	defer cp.RUnlock()
	v := cp.pyramid[rootCid.String()]
	mate := cp.mateData[rootCid.String()]
	cids := make([]*PyramidCidNum, 0, len(v)+len(mate))
	for overlay, c := range v {
		v := cp.chunk[overlay]
		if v > 1 {
			continue
		}
		over := boson.MustParseHexAddress(overlay)
		pcn := PyramidCidNum{Cid: over, Number: c.number}
		cids = append(cids, &pcn)
	}

	for overlay := range mate {
		v := cp.chunk[overlay]
		if v > 1 {
			continue
		}
		over := boson.MustParseHexAddress(overlay)
		if _, ok := cp.pyramid[rootCid.String()][overlay]; !ok {
			pcn := PyramidCidNum{Cid: over, Number: 1}
			cids = append(cids, &pcn)
		}
	}
	return cids
}

func (cp *chunkPyramid) getCidStore(rootCid, cid boson.Address) int {
	cp.RLock()
	defer cp.RUnlock()
	return cp.pyramid[rootCid.String()][cid.String()].sort
}

func (cp *chunkPyramid) updateCidSort(rootCid, cid boson.Address, sort int) {
	cp.Lock()
	defer cp.Unlock()
	v, ok := cp.pyramid[rootCid.String()][cid.String()]
	if !ok {
		return
	}
	v.sort = sort
	cp.pyramid[rootCid.String()][cid.String()] = v
}

func (cp *chunkPyramid) getRootChunk(rootCid string) int {
	cp.RLock()
	defer cp.RUnlock()

	for _, v := range cp.pyramid[rootCid] {
		if v.sort >= 0 {
			return *v.count
		}
	}
	return 0
}

func (cp *chunkPyramid) getRootHash(rootCID string) int {
	cp.RLock()
	defer cp.RUnlock()
	return len(cp.hashData[rootCID])
}

func (cp *chunkPyramid) delChunk(cid boson.Address) {
	v := cp.chunk[cid.String()]
	if v > 1 {
		cp.chunk[cid.String()] = v - 1
	} else {
		delete(cp.chunk, cid.String())
	}
}

func (cp *chunkPyramid) delRootCid(rootCID boson.Address) bool {
	cp.Lock()
	defer cp.Unlock()
	if cids, ok := cp.pyramid[rootCID.String()]; ok {
		hashs := cp.hashData[rootCID.String()]
		for _, cid := range hashs {
			cp.delChunk(boson.MustParseHexAddress(cid))
		}
		for cid := range cids {
			cp.delChunk(boson.MustParseHexAddress(cid))
		}
	}
	delete(cp.pyramid, rootCID.String())
	delete(cp.mateData, rootCID.String())
	delete(cp.hashData, rootCID.String())
	return true
}
