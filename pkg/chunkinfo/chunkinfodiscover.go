package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"math/rand"
	"strings"
	"sync"
)

var discoverKeyPrefix = "discover-"

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootcid:overlays:bitvector
	presence map[string]map[string]*bitvector.BitVector
	// rootcid:nodes
	overlays map[string][]boson.Address
}

func (ci *ChunkInfo) initChunkInfoDiscover() error {
	if err := ci.storer.Iterate(discoverKeyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), discoverKeyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(discoverKeyPrefix, key)
		if err != nil {
			return true, err
		}
		var vb bitVector
		if err := ci.storer.Get(key, &vb); err != nil {
			return true, err
		}
		bit, _ := bitvector.NewFromBytes(vb.B, vb.Len)
		ci.cd.putChunkInfoDiscover(rootCid, overlay, *bit)
		if err := ci.initChunkPyramid(context.Background(), rootCid); err != nil {
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func newChunkInfoDiscover() *chunkInfoDiscover {
	return &chunkInfoDiscover{overlays: make(map[string][]boson.Address), presence: make(map[string]map[string]*bitvector.BitVector)}
}

// isExists
func (cd *chunkInfoDiscover) isExists(rootCid boson.Address) bool {
	cd.RLock()
	defer cd.RUnlock()
	_, ok := cd.presence[rootCid.String()]
	return ok
}

// getChunkInfo
func (ci *ChunkInfo) getChunkInfo(rootCid, cid boson.Address) []aco.Route {
	ci.cd.RLock()
	defer ci.cd.RUnlock()
	res := make([]aco.Route, 0)
	for overlay, bv := range ci.cd.presence[rootCid.String()] {
		over := boson.MustParseHexAddress(overlay)
		s := ci.cp.getCidStore(rootCid, cid)
		if bv.Get(s) {
			route := aco.NewRoute(over, over)
			res = append(res, route)
		}
	}
	randOverlays := ci.getRandomChunkInfo(res)
	if randOverlays != nil && len(randOverlays) > 0 {
		res = append(res, randOverlays...)
	}
	return res
}

func (ci *ChunkInfo) getRandomChunkInfo(routes []aco.Route) []aco.Route {

	if len(routes) <= 0 {
		return nil
	}
	res := make([]aco.Route, 0)
	r := rand.Intn(len(routes))
	route := routes[r]
	overlays, errs := ci.route.GetTargetNeighbor(context.Background(), route.TargetNode, totalRouteCount)
	if errs != nil {
		ci.logger.Warningf("[chunk info discover] get peers: %w", errs)
		return res
	}
	for _, overlay := range overlays {
		v := aco.NewRoute(overlay, route.TargetNode)
		res = append(res, v)
	}
	return res
}

func (ci *ChunkInfo) getChunkInfoOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	ci.cd.RLock()
	defer ci.cd.RUnlock()
	res := make([]aurora.ChunkInfoOverlay, 0)
	for overlay, bit := range ci.cd.presence[rootCid.String()] {
		bv := aurora.BitVectorApi{Len: bit.Len(), B: bit.Bytes()}
		cio := aurora.ChunkInfoOverlay{Overlay: overlay, Bit: bv}
		res = append(res, cio)
	}
	return res
}

func (cd *chunkInfoDiscover) putChunkInfoDiscover(rootCid, overlay boson.Address, vector bitvector.BitVector) {
	cd.Lock()
	defer cd.Unlock()
	rc := rootCid.String()
	cd.overlays[rc] = append(cd.overlays[rc], overlay)
	if _, ok := cd.presence[rc]; !ok {
		cd.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	cd.presence[rc][overlay.String()] = &vector
}

func (ci *ChunkInfo) delDiscoverPresence(rootCid boson.Address) bool {
	ci.cd.Lock()
	defer ci.cd.Unlock()

	if v, ok := ci.cd.presence[rootCid.String()]; ok {
		for k := range v {
			err := ci.storer.Delete(generateKey(discoverKeyPrefix, rootCid, boson.MustParseHexAddress(k)))
			if err != nil {
				return false
			}
		}
	}

	delete(ci.cd.presence, rootCid.String())
	delete(ci.cd.overlays, rootCid.String())
	return true
}

// updateChunkInfo
func (ci *ChunkInfo) updateChunkInfo(rootCid, overlay boson.Address, bv []byte) {
	ci.cd.Lock()
	defer ci.cd.Unlock()

	rc := rootCid.String()
	over := overlay.String()
	if _, ok := ci.cd.presence[rc]; !ok {
		ci.cd.presence[rc] = make(map[string]*bitvector.BitVector)
	}
	if _, ok := ci.cd.presence[rootCid.String()][over]; !ok {
		ci.cd.overlays[rc] = append(ci.cd.overlays[rc], overlay)
	}
	vb, ok := ci.cd.presence[rc][overlay.String()]
	if !ok {
		v, _ := ci.getChunkSize(context.Background(), rootCid)
		if v == 0 {
			return
		}
		vb, _ = bitvector.NewFromBytes(bv, v)
		ci.cd.presence[rc][overlay.String()] = vb
	} else {
		if err := vb.SetBytes(bv); err != nil {
			ci.logger.Errorf("chunk discover: set bit vector error")
		}
	}
	// db
	if err := ci.storer.Put(generateKey(discoverKeyPrefix, rootCid, overlay), &bitVector{B: vb.Bytes(), Len: vb.Len()}); err != nil {
		ci.logger.Errorf("chunk discover: put store error")
	}
}

// createChunkInfoReq
func (cd *chunkInfoDiscover) createChunkInfoReq(rootCid, target, req boson.Address) pb.ChunkInfoReq {
	ciReq := pb.ChunkInfoReq{RootCid: rootCid.Bytes(), Target: target.Bytes(), Req: req.Bytes()}
	return ciReq
}

// doFindChunkInfo
func (ci *ChunkInfo) doFindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address) {
	ci.queueProcess(ctx, rootCid)
}
