package mock

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

type chunkPyramid struct {
	// rootCid:cid
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
}

type ChunkInfo struct {
	cp *chunkPyramid
}

func New() *ChunkInfo {
	return &ChunkInfo{
		cp: newChunkPyramid(),
	}
}

func (ci *ChunkInfo) FindChunkInfo(_ context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	panic("not implemented")
}

func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	panic("not implemented")
}

func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	panic("not implemented")
}

func (ci *ChunkInfo) OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays boson.Address) error {
	panic("not implemented")
}

func (ci *ChunkInfo) Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {
	panic("not implemented")
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid boson.Address) []*boson.Address {
	v := ci.cp.pyramid[rootCid.String()]
	cids := make([]*boson.Address, 0, len(v))
	for overlay := range v {
		over := boson.MustParseHexAddress(overlay)
		cids = append(cids, &over)
	}
	return cids
}

func (ci *ChunkInfo) IsDiscover(rootCid boson.Address) bool {
	return false
}

func (ci *ChunkInfo) PutChunkPyramid(rootCid, cid boson.Address, sort int) {
	rc := rootCid.String()
	if _, ok := ci.cp.pyramid[rc]; !ok {
		ci.cp.pyramid[rc] = make(map[string]int)
	}
	ci.cp.pyramid[rc][cid.String()] = sort
}