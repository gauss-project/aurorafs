package chunkinfo

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/resolver"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"resenje.org/singleflight"
)

type Interface interface {
	Discover(ctx context.Context, auth []byte, rootCid boson.Address) bool

	FindRoutes(ctx context.Context, rootCid boson.Address, cid boson.Address) []aco.Route

	OnRetrieved(ctx context.Context, rootCid boson.Address, cid boson.Address, bit int, overlay boson.Address) error

	OnTransferred(ctx context.Context, rootCid boson.Address, cid boson.Address, bit int, overlay, target boson.Address) error
}

type ChunkInfo struct {
	addr         boson.Address
	traversal    traversal.Traverser
	route        routetab.RouteTab
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	singleflight singleflight.Group
	oracleChain  chain.Resolver
	subPub       subscribe.SubPub

	queuesLk       sync.RWMutex
	queues         sync.Map // map[string]*queue
	syncLk         sync.RWMutex
	syncMsg        sync.Map // map[string]chan bool
	timeoutTrigger *timeoutTrigger
	pendingFinder  *pendingFinderInfo
	chunkStore     *localstore.DB
}

func New(addr boson.Address, streamer p2p.Streamer, logger logging.Logger, traversal traversal.Traverser,
	chunkStore *localstore.DB, storer storage.Storer, route routetab.RouteTab, oracleChain chain.Resolver,
	resolver resolver.Interface, subPub subscribe.SubPub) *ChunkInfo {
	chunkInfo := &ChunkInfo{
		addr:        addr,
		traversal:   traversal,
		route:       route,
		streamer:    streamer,
		logger:      logger,
		metrics:     newMetrics(),
		oracleChain: oracleChain,
		subPub:      subPub,

		timeoutTrigger: newTimeoutTrigger(),
		pendingFinder:  newPendingFinderInfo(),
		chunkStore:     chunkStore,
	}
	chunkInfo.triggerTimeOut()
	chunkInfo.cleanDiscoverTrigger()
	return chunkInfo
}

type BitVector struct {
	Len int    `json:"len"`
	B   []byte `json:"b"`
}

type BitVectorInfo struct {
	RootCid   boson.Address
	Overlay   boson.Address
	Bitvector BitVector
}

func (ci *ChunkInfo) Discover(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {
	key := fmt.Sprintf("%s%s", rootCid, "chunkinfo")
	topCtx := ctx
	v, _, _ := ci.singleflight.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		if ci.isDiscover(ctx, rootCid) {
			return true, nil
		}
		if ci.isDownload(ctx, rootCid, ci.addr) {
			return true, nil
		}
		overlays, _ := sctx.GetTargets(topCtx)
		if overlays == nil {
			overlays = ci.oracleChain.GetNodesFromCid(rootCid.Bytes())
			if len(overlays) <= 0 {
				return false, nil
			}
		}
		return ci.FindChunkInfo(context.Background(), authInfo, rootCid, overlays), nil
	})
	if v == nil {
		return false
	}
	return v.(bool)
}

func (ci *ChunkInfo) FindRoutes(ctx context.Context, rootCid boson.Address, cid boson.Address) []aco.Route {
	route, err := ci.getRoutes(rootCid, cid)
	if err != nil {
		ci.logger.Errorf("chunkInfo FindRoutes:%w", err)
		return nil
	}
	return route
}

func (ci *ChunkInfo) OnTransferred(ctx context.Context, rootCid boson.Address, cid boson.Address, bit int, overlay, target boson.Address) error {
	return ci.updateService(ctx, rootCid, cid, bit, overlay)
}

func (ci *ChunkInfo) OnRetrieved(ctx context.Context, rootCid boson.Address, cid boson.Address, bit int, overlay boson.Address) error {
	err := ci.updateService(ctx, rootCid, cid, bit, ci.addr)
	if err != nil {
		return err
	}
	err = ci.updateSource(ctx, rootCid, cid, bit, overlay)
	if err != nil {
		return err
	}

	return nil
}

func (ci *ChunkInfo) SubscribeDownloadProgress(notifier *rpc.Notifier, sub *rpc.Subscription, rootCids []boson.Address) {
	iNotifier := subscribe.NewNotifierWithDelay(notifier, sub, 1, true)
	for _, rootCid := range rootCids {
		_ = ci.subPub.Subscribe(iNotifier, "chunkInfo", "downloadProgress", rootCid.String())
	}
}

func (ci *ChunkInfo) SubscribeRetrievalProgress(notifier *rpc.Notifier, sub *rpc.Subscription, rootCid boson.Address) {
	iNotifier := subscribe.NewNotifierWithDelay(notifier, sub, 1, true)
	_ = ci.subPub.Subscribe(iNotifier, "chunkInfo", "retrievalProgress", rootCid.String())
}

func (ci *ChunkInfo) SubscribeRootCidStatus(notifier *rpc.Notifier, sub *rpc.Subscription) {
	iNotifier := subscribe.NewNotifierWithDelay(notifier, sub, 1, true)
	_ = ci.subPub.Subscribe(iNotifier, "chunkInfo", "rootCidStatus", "")
}

func (ci *ChunkInfo) PublishDownloadProgress(rootCid boson.Address, bitV BitVectorInfo) {
	_ = ci.subPub.Publish("chunkInfo", "downloadProgress", rootCid.String(), bitV)
}

func (ci *ChunkInfo) PublishRetrievalProgress(rootCid boson.Address, bitV BitVectorInfo) {
	_ = ci.subPub.Publish("chunkInfo", "retrievalProgress", rootCid.String(), bitV)
}

func (ci *ChunkInfo) PublishRootCidStatus(statusEvent RootCidStatusEven) {
	_ = ci.subPub.Publish("chunkInfo", "rootCidStatus", statusEvent.RootCid.String(), statusEvent)
}

func generateKey(keyPrefix string, rootCid, overlay boson.Address) string {
	return keyPrefix + rootCid.String() + "-" + overlay.String()
}

func unmarshalKey(keyPrefix, key string) (boson.Address, boson.Address, error) {
	addr := strings.TrimPrefix(key, keyPrefix)
	keys := strings.Split(addr, "-")
	rootCid, err := boson.ParseHexAddress(keys[0])
	if err != nil {
		return boson.ZeroAddress, boson.ZeroAddress, err
	}
	overlay, err := boson.ParseHexAddress(keys[1])
	if err != nil {
		return boson.ZeroAddress, boson.ZeroAddress, err
	}
	return rootCid, overlay, nil
}

func (ci *ChunkInfo) getCidSort(cid boson.Address, cid2 boson.Address) int {
	// TODO
	panic("TODO")
}
