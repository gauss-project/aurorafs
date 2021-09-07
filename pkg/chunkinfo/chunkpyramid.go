package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"strings"
	"sync"
)

var pyramidKeyPrefix = "pyramid-"

// chunkPyramid Pyramid
type chunkPyramid struct {
	sync.RWMutex
	// rootCid:cid
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
}

func (ci *ChunkInfo) initChunkPyramid() error {
	if err := ci.storer.Iterate(pyramidKeyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), pyramidKeyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, cid, err := unmarshalKey(pyramidKeyPrefix, key)
		if err != nil {
			return true, err
		}
		var sort int
		if err := ci.storer.Get(key, &sort); err != nil {
			return true, err
		}
		ci.cp.putChunkPyramid(rootCid, cid, sort)
		return false, nil
	}); err != nil {
		return err
	}
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

func (cp *chunkPyramid) putChunkPyramid(rootCid, cid boson.Address, sort int) {
	cp.Lock()
	defer cp.Unlock()
	rc := rootCid.String()
	if _, ok := cp.pyramid[rc]; !ok {
		cp.pyramid[rc] = make(map[string]int)
	}
	cp.pyramid[rc][cid.String()] = sort
}

// updateChunkPyramid
func (ci *ChunkInfo) updateChunkPyramid(rootCid boson.Address, pyramids [][][]byte, hashs [][]byte) {
	ci.cp.Lock()
	defer ci.cp.Unlock()
	py := make(map[string]int)
	var i int
	for _, p := range pyramids {
		for _, x := range p {
			py[boson.NewAddress(x).String()] = i
			// db
			ci.storer.Put(generateKey(pyramidKeyPrefix, rootCid, boson.NewAddress(x)), i)
			i++
		}
	}
	for _, hash := range hashs {
		py[boson.NewAddress(hash).String()] = -1
		// db
		ci.storer.Put(generateKey(pyramidKeyPrefix, rootCid, boson.NewAddress(hash)), -1)
	}
	ci.cp.pyramid[rootCid.String()] = py
}

// getChunkPyramid
func (ci *ChunkInfo) getChunkPyramid(cxt context.Context, rootCid boson.Address) (map[string][]byte, error) {
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
	var i int
	for _, t := range trie {
		for _, _ = range t {
			i++
		}
	}
	return i, nil
}

func (ci *ChunkInfo) getChunkPyramidHash(cxt context.Context, rootCid boson.Address) ([][]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	resp := make([][]byte, 0)
	for k := range v {
		resp = append(resp, []byte(k))
	}
	return resp, nil
}

func (ci *ChunkInfo) getChunkPyramidChunk(cxt context.Context, rootCid boson.Address, hash []byte) ([]byte, error) {
	v, err := ci.getChunkPyramid(cxt, rootCid)
	if err != nil {
		return nil, err
	}
	return v[string(hash)], nil
}

// doFindChunkPyramid
func (ci *ChunkInfo) doFindChunkPyramid(ctx context.Context, authInfo []byte, rootCid boson.Address, overlay boson.Address) error {
	if ci.cp.isExists(rootCid) {
		return nil
	}
	req := pb.ChunkPyramidHashReq{
		RootCid: rootCid.Bytes(),
	}
	resp, err := ci.sendPyramid(ctx, overlay, streamPyramidHashName, req)
	if err != nil {
		return err
	}
	return ci.onChunkPyramidResp(ctx, nil, boson.NewAddress(req.RootCid), overlay, resp.(pb.ChunkPyramidHashResp))
}

func (cp *chunkPyramid) getChunkCid(rootCid boson.Address) []*boson.Address {
	cp.RLock()
	defer cp.RUnlock()
	v := cp.pyramid[rootCid.String()]
	cids := make([]*boson.Address, 0, len(v))
	for overlay := range v {
		over := boson.MustParseHexAddress(overlay)
		cids = append(cids, &over)
	}
	return cids
}

func (cp *chunkPyramid) getCidStore(rootCid, cid boson.Address) int {
	cp.RLock()
	defer cp.RUnlock()
	return cp.pyramid[rootCid.String()][cid.String()]
}
