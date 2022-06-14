package chunkinfo

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
)

const (
	cleanTime = 1 * time.Hour
	maxTime   = 24 * 60 * 60
)

func (ci *ChunkInfo) isDiscover(ctx context.Context, rootCid boson.Address) bool {
	consumerList, err := ci.chunkStore.Get(chunkstore.DISCOVER, rootCid)
	if err != nil {
		ci.logger.Errorf("chunkInfo isDiscover:%w", err)
		return false
	}
	if len(consumerList) <= 0 {
		return false
	}
	return true
}

func (ci *ChunkInfo) getRoutes(rootCid, cid boson.Address) ([]aco.Route, error) {
	res := make([]aco.Route, 0)
	consumerList, err := ci.chunkStore.Get(chunkstore.DISCOVER, rootCid)
	if err != nil {
		return nil, err
	}

	s := ci.getCidSort(rootCid, cid)
	for _, c := range consumerList {
		bv, err := bitvector.NewFromBytes(c.B, c.Len)
		if err != nil {
			return nil, err
		}
		if bv.Get(s) {
			route := aco.NewRoute(c.Overlay, c.Overlay)
			res = append(res, route)
		}
	}
	res = ci.addRoutes(res)
	return res, nil
}

func (ci *ChunkInfo) addRoutes(routes []aco.Route) []aco.Route {
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

func (ci *ChunkInfo) getDiscover(rootCid boson.Address) ([]aurora.ChunkInfoOverlay, error) {
	res := make([]aurora.ChunkInfoOverlay, 0)
	consumerList, err := ci.chunkStore.Get(chunkstore.DISCOVER, rootCid)
	if err != nil {
		return nil, err
	}
	for _, c := range consumerList {
		bv := aurora.BitVectorApi{B: c.B, Len: c.Len}
		cio := aurora.ChunkInfoOverlay{Overlay: c.Overlay.String(), Bit: bv}
		res = append(res, cio)
	}
	return res, nil
}

func (ci *ChunkInfo) removeDiscover(rootCid boson.Address) error {
	return ci.chunkStore.RemoveAll(chunkstore.DISCOVER, rootCid)
}

func (ci *ChunkInfo) updateDiscover(ctx context.Context, rootCid, overlay boson.Address, bv []byte) error {
	var provider chunkstore.Provider
	provider.B = bv
	provider.Len = len(bv) * 8
	provider.Overlay = overlay
	return ci.chunkStore.Put(chunkstore.DISCOVER, rootCid, []chunkstore.Provider{provider})
}

func (ci *ChunkInfo) FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) bool {
	msgChan := make(chan bool, 1)
	for {
		ci.syncMsg.Store(rootCid.String(), msgChan)
		ci.findChunkInfo(ctx, authInfo, rootCid, overlays)
		select {
		case <-ctx.Done():
			return false
		case msg := <-msgChan:
			ci.syncMsg.Delete(rootCid.String())
			return msg
		}
	}
}

func (ci *ChunkInfo) findChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	ci.pendingFinder.updatePendingFinder(rootCid)
	if ci.getQueue(rootCid.String()) == nil {
		ci.newQueue(rootCid.String())
	}
	for _, overlay := range overlays {
		if ci.getQueue(rootCid.String()).isExists(Pulled, overlay.Bytes()) || ci.getQueue(rootCid.String()).isExists(Pulling, overlay.Bytes()) ||
			ci.getQueue(rootCid.String()).isExists(UnPull, overlay.Bytes()) {
			continue
		}
		ci.getQueue(rootCid.String()).push(UnPull, overlay.Bytes())
	}
	go ci.doFindChunkInfo(ctx, authInfo, rootCid)
}

func (ci *ChunkInfo) doFindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address) {
	ci.queueProcess(ctx, rootCid)
}

func (ci *ChunkInfo) cleanDiscoverTrigger() {
	t := time.NewTicker(cleanTime)
	go func() {
		for {
			<-t.C
			now := time.Now().Unix()
			ctx := context.Background()
			discover, err := ci.chunkStore.GetAll(chunkstore.DISCOVER)
			if err != nil {
				ci.logger.Errorf("chunkInfo cleanDiscover get discover:%w", err)
				continue
			}
			for rCid, providerList := range discover {
				rootCid := boson.MustParseHexAddress(rCid)
				if ci.isDownload(ctx, rootCid, ci.addr) {
					ci.syncLk.Lock()
					ci.cancelPendingFindInfo(rootCid)
					ci.queues.Delete(rootCid.String())
					ci.syncLk.Unlock()
					err = ci.chunkStore.RemoveAll(chunkstore.DISCOVER, rootCid)
					if err != nil {
						ci.logger.Errorf("chunkInfo cleanDiscover remove discover:%w", err)
					}
					break
				}
				for _, provider := range providerList {
					if provider.Time+maxTime < now {
						err = ci.chunkStore.Remove(chunkstore.DISCOVER, rootCid, provider.Overlay)
						if err != nil {
							ci.logger.Errorf("chunkInfo cleanDiscover remove discover:%w", err)
						}
						if q, ok := ci.queues.Load(rootCid); ok {
							q.(*queue).popNode(Pulled, provider.Overlay.Bytes())
						}
					}
				}
			}
		}
	}()
}

func (ci *ChunkInfo) cancelPendingFindInfo(rootCid boson.Address) {
	ci.pendingFinder.cancelPendingFinder(rootCid)
}
