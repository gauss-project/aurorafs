package mock

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
)

type chunkPyramid struct {
	// rootCid:cid
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
}

type pendingFinderInfo struct {
	// rootCid
	finder map[string]struct{}
}

func newPendingFinderInfo() *pendingFinderInfo {
	return &pendingFinderInfo{finder: make(map[string]struct{})}
}

type ChunkInfo struct {
	cp    *chunkPyramid
	cpd   *pendingFinderInfo
	queue map[string]chunkinfo.Pull
}

func New() *ChunkInfo {
	return &ChunkInfo{
		cp:    newChunkPyramid(),
		cpd:   newPendingFinderInfo(),
		queue: make(map[string]chunkinfo.Pull),
	}
}

func (ci *ChunkInfo) FindChunkInfo(_ context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	panic("not implemented")
}

func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	panic("not implemented")
}

func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	if _, ok := ci.cpd.finder[rootCid.String()]; ok {
		delete(ci.cpd.finder, rootCid.String())
	}

	if _, ok := ci.queue[rootCid.String()]; ok {
		delete(ci.queue, rootCid.String())
	}
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
	if _, ok := ci.cpd.finder[rootCid.String()]; ok {
		return true
	}

	status, ok := ci.queue[rootCid.String()]
	if ok && status != chunkinfo.Pulled {
		return true
	}

	return false
}
func (ci *ChunkInfo) GetFileList(overlay boson.Address) (fileListInfo map[string]*aurora.FileInfo, rootList []boson.Address) {
	return nil, nil
}

func (ci *ChunkInfo) PutChunkPyramid(rootCid, cid boson.Address, sort int) {
	rc := rootCid.String()
	if _, ok := ci.cp.pyramid[rc]; !ok {
		ci.cp.pyramid[rc] = make(map[string]int)
	}
	ci.cp.pyramid[rc][cid.String()] = sort
}

func (ci *ChunkInfo) ChangeDiscoverStatus(rootCid boson.Address, s chunkinfo.Pull) {
	if _, ok := ci.queue[rootCid.String()]; !ok {
		ci.queue[rootCid.String()] = s
	}

	if s != chunkinfo.Pulled {
		ci.cpd.finder[rootCid.String()] = struct{}{}
	} else {
		ci.CancelFindChunkInfo(rootCid)
	}
}

func (ci *ChunkInfo) DelFile(rootCid, overlay boson.Address) bool {
	return true
}
