package mock

import (
	"context"
	"fmt"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"github.com/gauss-project/aurorafs/pkg/routetab/mock"
)

var (
	chunkMap map[string][]aco.Route
	mu       sync.Mutex
)

func init() {
	chunkMap = make(map[string][]aco.Route)
}

type chunkPyramid struct {
	// rootCid:cid
	pyramid map[string]map[string]int
}

func newChunkPyramid() *chunkPyramid {
	return &chunkPyramid{pyramid: make(map[string]map[string]int)}
}

func (cp *chunkPyramid) delRootCid(rootCID boson.Address) bool {
	delete(cp.pyramid, rootCID.String())
	return true
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
	route mock.MockRouteTable
	queue map[string]chunkinfo.Pull
}

func New(route mock.MockRouteTable) *ChunkInfo {
	chunkMap = make(map[string][]aco.Route)
	return &ChunkInfo{
		route: route,
		cp:    newChunkPyramid(),
		cpd:   newPendingFinderInfo(),
		queue: make(map[string]chunkinfo.Pull),
	}
}

func (ci *ChunkInfo) FindChunkInfo(_ context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) bool {
	panic("not implemented")
}

func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) []aco.Route {
	mapKey := fmt.Sprintf("%v,%v", rootCid.String(), cid.String())
	for k, v := range ci.route.NeighborMap {
		for _, n := range v {
			route := aco.NewRoute(n, boson.MustParseHexAddress(k))
			chunkMap[mapKey] = append(chunkMap[mapKey], route)
		}
	}
	return chunkMap[mapKey]
}

func (ci *ChunkInfo) GetChunkInfoDiscoverOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	panic("not implemented")
}

func (ci *ChunkInfo) GetChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	panic("not implemented")
}

func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	delete(ci.cpd.finder, rootCid.String())

	delete(ci.queue, rootCid.String())
}

func (ci *ChunkInfo) OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays, target boson.Address) error {
	mapKey := fmt.Sprintf("%v,%v", rootCid.String(), cid.String())
	mu.Lock()
	if _, exist := chunkMap[mapKey]; !exist {
		chunkMap[mapKey] = make([]aco.Route, 0)
	}
	route := aco.NewRoute(overlays, overlays)
	chunkMap[mapKey] = append(chunkMap[mapKey], route)
	mu.Unlock()
	return nil
}

func (ci *ChunkInfo) Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {
	return true
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid boson.Address) []*chunkinfo.PyramidCidNum {
	v := ci.cp.pyramid[rootCid.String()]
	cids := make([]*chunkinfo.PyramidCidNum, 0, len(v))
	for overlay, cnt := range v {
		over := boson.MustParseHexAddress(overlay)
		cids = append(cids, &chunkinfo.PyramidCidNum{
			Cid:    over,
			Number: cnt,
		})
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
	ci.cp.pyramid[rc][cid.String()]++
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

func (ci *ChunkInfo) DelFile(rootCid boson.Address) bool {
	return true
}

func (ci *ChunkInfo) DelDiscover(rootCid boson.Address) {

}

func (ci *ChunkInfo) OnChunkRetrieved(cid, rootCid, sourceOverlay boson.Address) error {
	return nil
}

func (ci *ChunkInfo) GetChunkInfoSource(rootCid boson.Address) aurora.ChunkInfoSourceApi {
	return aurora.ChunkInfoSourceApi{}
}
