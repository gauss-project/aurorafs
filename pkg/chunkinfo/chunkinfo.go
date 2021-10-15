package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap/oracle"
	"resenje.org/singleflight"
	"strings"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

type Interface interface {
	FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) bool

	GetChunkInfo(rootCid boson.Address, cid boson.Address) []aco.Route

	GetChunkInfoDiscoverOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay

	GetChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay

	CancelFindChunkInfo(rootCid boson.Address)

	OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays, target boson.Address) error

	Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool

	GetChunkPyramid(rootCid boson.Address) []*PyramidCidNum

	IsDiscover(rootCid boson.Address) bool

	GetFileList(overlay boson.Address) (fileListInfo map[string]*aurora.FileInfo, rootList []boson.Address)

	DelFile(rootCid boson.Address) bool

	DelDiscover(rootCid boson.Address)

	DelPyramid(rootCid boson.Address) bool
}

// ChunkInfo
type ChunkInfo struct {
	addr         boson.Address
	storer       storage.StateStorer
	traversal    traversal.Service
	route        routetab.RouteTab
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	t            *time.Ticker
	tt           *timeoutTrigger
	queuesLk     sync.RWMutex
	queues       map[string]*queue
	syncLk       sync.RWMutex
	syncMsg      map[string]chan bool
	ct           *chunkInfoTabNeighbor
	cd           *chunkInfoDiscover
	cp           *chunkPyramid
	cpd          *pendingFinderInfo
	singleflight singleflight.Group
	oracleChain  oracle.Resolver
}

// New
func New(addr boson.Address, streamer p2p.Streamer, logger logging.Logger, traversal traversal.Service, storer storage.StateStorer, route routetab.RouteTab, oracleChain oracle.Resolver) *ChunkInfo {
	queues := make(map[string]*queue)
	syncMsg := make(map[string]chan bool)

	chunkinfo := &ChunkInfo{
		addr:        addr,
		storer:      storer,
		route:       route,
		metrics:     newMetrics(),
		ct:          newChunkInfoTabNeighbor(),
		cd:          newChunkInfoDiscover(),
		cp:          newChunkPyramid(),
		cpd:         newPendingFinderInfo(),
		queues:      queues,
		syncMsg:     syncMsg,
		tt:          newTimeoutTrigger(),
		streamer:    streamer,
		logger:      logger,
		traversal:   traversal,
		oracleChain: oracleChain,
	}
	chunkinfo.triggerTimeOut()
	chunkinfo.cleanDiscoverTrigger()
	return chunkinfo
}

const chunkInfoRetryIntervalDuration = 1 * time.Second

type Response struct {
	StatusCode int             `json:"code"`
	Message    string          `json:"msg"`
	Body       RootCIDResponse `json:"data"`
}

type RootCIDResponse struct {
	RootCID   string   `json:"rootcid"`
	Addresses []string `json:"addresses"`
}

type bitVector struct {
	Len int    `json:"len"`
	B   []byte `json:"b"`
}

func (ci *ChunkInfo) InitChunkInfo() error {
	if err := ci.initChunkInfoTabNeighbor(); err != nil {
		return err
	}
	if err := ci.initChunkInfoDiscover(); err != nil {
		return err
	}
	return nil
}

func (ci *ChunkInfo) Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {

	key := fmt.Sprintf("%s%s", rootCid, "chunkinfo")
	v, _, _ := ci.singleflight.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		if ci.ct.isExists(rootCid, ci.addr) {
			return true, nil
		}
		if ci.cd.isExists(rootCid) {
			return true, nil
		}
		overlays := ci.oracleChain.GetNodesFromCid(rootCid.Bytes())
		if len(overlays) <= 0 {
			return false, nil
		}
		return ci.FindChunkInfo(context.Background(), authInfo, rootCid, overlays), nil
	})
	return v.(bool)
}

// FindChunkInfo
func (ci *ChunkInfo) FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) bool {
	ticker := time.NewTicker(chunkInfoRetryIntervalDuration)
	defer ticker.Stop()
	count := len(overlays)

	var (
		peerAttempt  int
		peersResults int
		errorC       = make(chan error, count)
		msgChan      = make(chan bool, 1)
	)
	for {
		if ci.cp.isExists(rootCid) {
			ticker.Stop()
			ci.syncLk.Lock()
			ci.syncMsg[rootCid.String()] = msgChan
			ci.syncLk.Unlock()
			ci.findChunkInfo(ctx, authInfo, rootCid, overlays)
		} else if peerAttempt < count {
			if err := ci.doFindChunkPyramid(ctx, nil, rootCid, overlays[peerAttempt]); err != nil {
				errorC <- err
			}
			peerAttempt++
		}

		select {
		case <-ticker.C:
		case <-errorC:
			peersResults++
		case <-ctx.Done():
			return false
		case msg := <-msgChan:
			ci.syncLk.Lock()
			delete(ci.syncMsg, rootCid.String())
			ci.syncLk.Unlock()
			return msg
		}

		if peersResults >= count {
			return false
		}
	}
}

func (ci *ChunkInfo) findChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {

	ci.cpd.updatePendingFinder(rootCid)
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

// GetChunkInfo
func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) []aco.Route {
	return ci.getChunkInfo(rootCid, cid)
}

func (ci *ChunkInfo) GetChunkInfoDiscoverOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	return ci.getChunkInfoOverlays(rootCid)
}

func (ci *ChunkInfo) GetChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	return ci.getChunkInfoServerOverlays(rootCid)
}

// CancelFindChunkInfo
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred
func (ci *ChunkInfo) OnChunkTransferred(cid, rootCid boson.Address, overlay, target boson.Address) error {
	return ci.updateNeighborChunkInfo(rootCid, cid, overlay, target)
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid boson.Address) []*PyramidCidNum {
	return ci.cp.getChunkCid(rootCid)
}

func (ci *ChunkInfo) IsDiscover(rootCid boson.Address) bool {
	q := ci.getQueue(rootCid.String())
	if q == nil {
		return false
	}
	if ci.cpd.getPendingFinder(rootCid) {
		return true
	}
	if q.len(Pulled)+q.len(Pulling) >= PullMax {
		return true
	}
	if q.len(UnPull)+q.len(Pulling) == 0 && q.len(Pulled) > 0 {
		return true
	}

	return false
}

func (ci *ChunkInfo) GetFileList(overlay boson.Address) (fileListInfo map[string]*aurora.FileInfo, rootList []boson.Address) {
	ci.ct.RLock()
	defer ci.ct.RUnlock()
	chunkInfo := ci.ct.presence
	fileListInfo = make(map[string]*aurora.FileInfo)
	for root, node := range chunkInfo {
		if v, ok := node[overlay.String()]; ok {
			file := &aurora.FileInfo{}
			file.PinState = false
			file.TreeSize = ci.cp.getRootHash(root)
			file.FileSize = ci.cp.getRootChunk(root)
			file.Bitvector.B = v.Bytes()
			file.Bitvector.Len = v.Len()
			fileListInfo[root] = file
			rootList = append(rootList, boson.MustParseHexAddress(root))
		}
	}
	return
}

func (ci *ChunkInfo) DelFile(rootCid boson.Address) bool {
	ci.queuesLk.Lock()
	ci.CancelFindChunkInfo(rootCid)
	delete(ci.queues, rootCid.String())
	ci.queuesLk.Unlock()

	if !ci.delDiscoverPresence(rootCid) {
		return false
	}
	return ci.delPresence(rootCid)
}

func (ci *ChunkInfo) DelDiscover(rootCid boson.Address) {
	ci.queuesLk.Lock()
	ci.CancelFindChunkInfo(rootCid)
	delete(ci.queues, rootCid.String())
	ci.queuesLk.Unlock()
	ci.delDiscoverPresence(rootCid)
}

func (ci *ChunkInfo) DelPyramid(rootCid boson.Address) bool {
	return ci.cp.delRootCid(rootCid)
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
