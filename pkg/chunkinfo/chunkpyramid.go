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
	hashCount map[string]int
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
	return &chunkPyramid{pyramid: make(map[string]map[string]pyramidCidCount), mateData: make(map[string]map[string][]byte), hashCount: make(map[string]int)}
}

func (ci *ChunkInfo) initChunkPyramid(ctx context.Context, rootCid boson.Address) error {
	if ci.cp.pyramid[rootCid.String()] != nil {
		return nil
	}
	trie, err := ci.traversal.GetPyramid(ctx, rootCid)
	if err != nil {
		return err
	}
	data, err := ci.traversal.GetChunkHashes(ctx, rootCid, trie)
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
	var i, max, hashMax int
	for _, p := range pyramids {
		for _, x := range p {
			if v, ok := py[boson.NewAddress(x).String()]; !ok {
				py[boson.NewAddress(x).String()] = pyramidCidCount{
					sort:   i,
					count:  &max,
					number: 1,
				}
				i++
			} else {
				v.number = v.number + 1
				py[boson.NewAddress(x).String()] = v
			}
			max++
		}
	}
	for k := range trie {
		if _, ok := py[k]; !ok {
			hashMax++
		}
	}
	ci.cp.pyramid[rootCid.String()] = py
	ci.cp.hashCount[rootCid.String()] = hashMax
	ci.cp.mateData[rootCid.String()] = trie
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
	trie, err := ci.traversal.GetChunkHashes(cxt, rootCid, v)
	if err != nil {
		return 0, err
	}
	if ci.cp.pyramid[rootCid.String()] == nil {
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
	resp, err := ci.sendPyramids(ctx, overlay, streamPyramidName, req)
	if err != nil {
		return err
	}
	return ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.RootCid), resp.([]pb.ChunkPyramidResp))
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
	return cp.hashCount[rootCID]
}
func (cp *chunkPyramid) delRootCid(rootCID boson.Address) bool {
	cp.Lock()
	defer cp.Unlock()
	delete(cp.pyramid, rootCID.String())
	delete(cp.mateData, rootCID.String())
	delete(cp.hashCount, rootCID.String())
	return true
}
