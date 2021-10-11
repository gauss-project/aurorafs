package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"strings"
	"sync"
	"time"
)

var discoverKeyPrefix = "discover-"

const (
	cleanTime = 1 * time.Hour
	maxTime   = 24 * 60 * 60
)

// chunkInfoDiscover
type chunkInfoDiscover struct {
	sync.RWMutex
	// rootcid:overlays:bitvector
	presence map[string]map[string]*discoverBitVector
}

type discoverBitVector struct {
	bit  *bitvector.BitVector
	time int64
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
	return &chunkInfoDiscover{presence: make(map[string]map[string]*discoverBitVector)}
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
		if bv.bit.Get(s) {
			route := aco.NewRoute(over, over)
			res = append(res, route)
		}
	}
	res = ci.getRandomChunkInfo(res)
	return res
}

func (ci *ChunkInfo) getRandomChunkInfo(routes []aco.Route) []aco.Route {

	if len(routes) <= 0 {
		return nil
	}
	res := make([]aco.Route, 0)
	for _, route := range routes {
		overlays, errs := ci.route.GetTargetNeighbor(context.Background(), route.TargetNode, totalRouteCount)
		if errs != nil || overlays == nil {
			res = append(res, route)
			continue
		}
		for _, overlay := range overlays {
			v := aco.NewRoute(overlay, route.TargetNode)
			res = append(res, v)
		}
	}
	return res
}

func (ci *ChunkInfo) getChunkInfoOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	ci.cd.RLock()
	defer ci.cd.RUnlock()
	res := make([]aurora.ChunkInfoOverlay, 0)
	for overlay, bv := range ci.cd.presence[rootCid.String()] {
		bv := aurora.BitVectorApi{Len: bv.bit.Len(), B: bv.bit.Bytes()}
		cio := aurora.ChunkInfoOverlay{Overlay: overlay, Bit: bv}
		res = append(res, cio)
	}
	return res
}

func (cd *chunkInfoDiscover) putChunkInfoDiscover(rootCid, overlay boson.Address, vector bitvector.BitVector) {
	cd.Lock()
	defer cd.Unlock()
	rc := rootCid.String()
	if _, ok := cd.presence[rc]; !ok {
		cd.presence[rc] = make(map[string]*discoverBitVector)
	}
	db := &discoverBitVector{
		bit:  &vector,
		time: time.Now().Unix(),
	}
	cd.presence[rc][overlay.String()] = db
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
	return true
}

// updateChunkInfo
func (ci *ChunkInfo) updateChunkInfo(rootCid, overlay boson.Address, bv []byte) {
	ci.cd.Lock()
	defer ci.cd.Unlock()

	rc := rootCid.String()
	if _, ok := ci.cd.presence[rc]; !ok {
		ci.cd.presence[rc] = make(map[string]*discoverBitVector)
	}
	vb, ok := ci.cd.presence[rc][overlay.String()]
	if !ok {
		v, _ := ci.getChunkSize(context.Background(), rootCid)
		if v == 0 {
			return
		}
		bit, _ := bitvector.NewFromBytes(bv, v)
		vb = &discoverBitVector{
			bit:  bit,
			time: time.Now().Unix(),
		}
		ci.cd.presence[rc][overlay.String()] = vb
	} else {
		if err := vb.bit.SetBytes(bv); err != nil {
			ci.logger.Errorf("chunk discover: set bit vector error")
		}
	}
	// db
	if err := ci.storer.Put(generateKey(discoverKeyPrefix, rootCid, overlay), &bitVector{B: vb.bit.Bytes(), Len: vb.bit.Len()}); err != nil {
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

func (ci *ChunkInfo) cleanDiscoverTrigger() {
	t := time.NewTicker(cleanTime)
	go func() {
		for {
			<-t.C
			now := time.Now().Unix()
			ci.cd.Lock()
			for rootCid, discover := range ci.cd.presence {
				for overlay, db := range discover {
					if db.time+maxTime < now {
						delete(discover, overlay)
						if err := ci.storer.Delete(generateKey(discoverKeyPrefix, boson.MustParseHexAddress(rootCid), boson.MustParseHexAddress(overlay))); err != nil {
							continue
						}
						ci.queuesLk.Lock()
						if q, ok := ci.queues[rootCid]; ok {
							q.popNode(Pulled, boson.MustParseHexAddress(overlay).Bytes())
						}
						ci.queuesLk.Unlock()
					}
				}
			}
			ci.cd.Unlock()
		}
	}()
}
