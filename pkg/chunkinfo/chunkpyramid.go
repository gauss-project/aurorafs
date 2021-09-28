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
}

type pyramidCidCount struct {
	sort   int
	number int
}

type PyramidCidNum struct {
	Cid    boson.Address
	Number int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]pyramidCidCount)}
}

func (ci *ChunkInfo) initChunkPyramid(ctx context.Context, rootCid boson.Address) error {
	if ci.cp.pyramid[rootCid.String()] != nil {
		return nil
	}
	trie, err := ci.traversal.GetTrieData(ctx, rootCid)
	if err != nil {
		return err
	}
	data, err := ci.traversal.CheckTrieData(ctx, rootCid, trie)
	if err != nil {
		return err
	}
	hashs := make([][]byte, 0)
	for k := range trie {
		hashs = append(hashs, boson.MustParseHexAddress(k).Bytes())
	}
	ci.updateChunkPyramid(rootCid, data, hashs)
	return nil
}

func (cp *chunkPyramid) checkPyramid(rootCid, cid boson.Address) bool {
	cp.RLock()
	defer cp.RUnlock()
	if cp.pyramid[rootCid.String()] != nil {
		_, ok := cp.pyramid[rootCid.String()][cid.String()]
		return ok
	}
	return false
}

// updateChunkPyramid
func (ci *ChunkInfo) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte, hashs [][]byte) {
	ci.cp.Lock()
	defer ci.cp.Unlock()
	py := make(map[string]pyramidCidCount)
	var i, hashMax int
	for _, p := range pyramids {
		for _, x := range p {
			if v, ok := py[boson.NewAddress(x).String()]; !ok {
				py[boson.NewAddress(x).String()] = pyramidCidCount{
					sort:   i,
					number: 1,
				}
				i++
			} else {
				v.number = v.number + 1
				py[boson.NewAddress(x).String()] = v
			}
		}
	}

	for _, hash := range hashs {
		if _, ok := py[boson.NewAddress(hash).String()]; ok {
			continue
		}
		py[boson.NewAddress(hash).String()] = pyramidCidCount{
			sort:   -1,
			number: 1,
		}
		hashMax++
	}
	ci.cp.pyramid[rootCid.String()] = py
}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
	ci.cp.RLock()
	defer ci.cp.RUnlock()
	v, err := ci.traversal.GetTrieData(cxt, rootCid)
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
	trie, err := ci.traversal.CheckTrieData(cxt, rootCid, v)
	if err != nil {
		return 0, err
	}
	if ci.cp.pyramid[rootCid.String()] == nil {
		hashs := make([][]byte, 0)
		for k := range v {
			hashs = append(hashs, boson.MustParseHexAddress(k).Bytes())
		}
		ci.updateChunkPyramid(rootCid, trie, hashs)
	}
	var max = 1
	for _, i := range ci.cp.pyramid[rootCid.String()] {
		if max < i.sort {
			max = i.sort
		}
	}
	return max, nil
}

func (ci *ChunkInfo) getChunkPyramidHash(cxt context.Context, rootCid boson.Address) ([][]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	resp := make([][]byte, 0)
	for k := range v {
		resp = append(resp, boson.MustParseHexAddress(k).Bytes())
	}
	return resp, nil
}

func (ci *ChunkInfo) getChunkPyramidChunk(cxt context.Context, rootCid boson.Address, hash []byte) ([]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v[boson.NewAddress(hash).String()], nil
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlay boson.Address) error {
	if ci.cp.isExists(rootCid) {
		return nil
	}
	req := pb.ChunkPyramidHashReq{
		RootCid: rootCid.Bytes(),
		Target:  overlay.Bytes(),
	}
	resp, target, err := ci.sendPyramids(ctx, overlay, streamPyramidHashName, req)
	if err != nil {
		return err
	}
	return ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.RootCid), target, resp.(pb.ChunkPyramidHashResp))
}

func (cp *chunkPyramid) getChunkCid(rootCid boson.Address) []*PyramidCidNum {
	cp.RLock()
	defer cp.RUnlock()
	v := cp.pyramid[rootCid.String()]
	cids := make([]*PyramidCidNum, 0, len(v))
	for overlay, c := range v {
		over := boson.MustParseHexAddress(overlay)
		pcn := PyramidCidNum{Cid: over, Number: c.number}
		cids = append(cids, &pcn)
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

	sort := 0
	for _, v := range cp.pyramid[rootCid] {
		if v.sort >= 0 {
			sort++
		}
	}
	return sort
}

func (cp *chunkPyramid) getRootHash(rootCID string) int {
	cp.RLock()
	defer cp.RUnlock()
	count := 0
	for _, v := range cp.pyramid[rootCID] {
		if v.sort < 0 {
			count++
		}
	}
	return count
}
func (cp *chunkPyramid) delRootCid(rootCID boson.Address) bool {
	cp.Lock()
	defer cp.Unlock()
	delete(cp.pyramid, rootCID.String())
	return true
}
