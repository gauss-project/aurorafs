package chunkinfo

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
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
	presence        map[string]map[string]*discoverBitVector
	discoverPutChan chan chunkPut
}

type discoverBitVector struct {
	bit  *bitvector.BitVector
	time int64
}

func (ci *ChunkInfo) initChunkInfoDiscover() error {
	if err := ci.stateStorer.Iterate(discoverKeyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), discoverKeyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(discoverKeyPrefix, key)
		if err != nil {
			return false, err
		}
		var vb BitVector
		if err := ci.stateStorer.Get(key, &vb); err != nil {
			return false, err
		}
		bit, _ := bitvector.NewFromBytes(vb.B, vb.Len)
		if err := ci.chunkPutChanUpdate(context.Background(), ci.cp, ci.initChunkPyramid, context.Background(), rootCid).err; err != nil {
			return false, nil
		}
		ci.cd.putChunkInfoDiscover(rootCid, overlay, *bit)
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func newChunkInfoDiscover() *chunkInfoDiscover {
	discover := &chunkInfoDiscover{presence: make(map[string]map[string]*discoverBitVector),
		discoverPutChan: make(chan chunkPut, 1000),
	}

	return discover
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
		s := ci.getCidSort(rootCid, cid)
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
		return routes
	}
	res := make([]aco.Route, 0)
	ctx := context.Background()
	for _, route := range routes {
		overlays, errs := ci.route.GetTargetNeighbor(ctx, route.TargetNode, totalRouteCount)
		if errs != nil || overlays == nil {
			continue
		}
		for _, overlay := range overlays {
			v := aco.NewRoute(overlay, route.TargetNode)
			res = append(res, v)
		}
	}
	if len(res) == 0 {
		return routes
	}
	exist := make(map[string]struct{})
	for _, overlay := range res {
		for _, i := range routes {
			if i.TargetNode.Equal(overlay.LinkNode) && i.TargetNode.Equal(i.LinkNode) {
				continue
			}
			if _, e := exist[i.LinkNode.String()]; !e {
				res = append(res, i)
			}
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
	if v, ok := ci.cd.presence[rootCid.String()]; ok {
		for k := range v {
			err := ci.stateStorer.Delete(generateKey(discoverKeyPrefix, rootCid, boson.MustParseHexAddress(k)))
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
	if err := ci.stateStorer.Put(generateKey(discoverKeyPrefix, rootCid, overlay), &BitVector{B: vb.bit.Bytes(), Len: vb.bit.Len()}); err != nil {
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
			for rootCid, discover := range ci.cd.presence {
				rc := boson.MustParseHexAddress(rootCid)
				if ci.ct.isDownload(rc, ci.addr) {
					ci.DelDiscover(rc)
				}
				for overlay, db := range discover {
					if db.time+maxTime < now {
						delete(discover, overlay)
						if err := ci.stateStorer.Delete(generateKey(discoverKeyPrefix, rc, boson.MustParseHexAddress(overlay))); err != nil {
							continue
						}
						if q, ok := ci.queues.Load(rootCid); ok {
							q.(*queue).popNode(Pulled, boson.MustParseHexAddress(overlay).Bytes())
						}
					}
				}
			}
		}
	}()
}

func (cd *chunkInfoDiscover) setLock() {
	cd.Lock()
}
func (cd *chunkInfoDiscover) setUnLock() {
	cd.Unlock()
}
func (cd *chunkInfoDiscover) getChan() chan chunkPut {
	return cd.discoverPutChan
}
