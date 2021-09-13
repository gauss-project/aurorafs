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
	// rootCid:cid
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
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
		hashs = append(hashs, []byte(k))
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
			i++
		}
	}
	for _, hash := range hashs {
		py[boson.NewAddress(hash).String()] = -1
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
	if ci.cp.pyramid[rootCid.String()] == nil {
		hashs := make([][]byte, 0)
		for k := range v {
			hashs = append(hashs, []byte(k))
		}
		ci.updateChunkPyramid(rootCid, trie, hashs)
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
		Target:  overlay.Bytes(),
	}
	resp, err := ci.sendPyramids(ctx, overlay, streamPyramidHashName, req)
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

func (cp *chunkPyramid) getRootHash(rootCID string) int {
	cp.RLock()
	defer cp.RUnlock()
	if cid, ok := cp.pyramid[rootCID]; ok {
		return len(cid)
	}
	return 0
}
func (cp *chunkPyramid) delRootCid(rootCID boson.Address) bool {
	cp.Lock()
	defer cp.Unlock()
	delete(cp.pyramid, rootCID.String())
	return true
}
