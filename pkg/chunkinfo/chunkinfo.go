package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"reflect"
	"resenje.org/singleflight"
	"runtime"
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

	DelFile(rootCid boson.Address, del func()) bool

	DelDiscover(rootCid boson.Address)

	OnChunkRetrieved(cid, rootCid, sourceOverlay boson.Address) error

	GetChunkInfoSource(rootCid boson.Address) aurora.ChunkInfoSourceApi
}

type chunkPutEntry interface {
	setLock()
	setUnLock()
	getChan() chan chunkPut
}
type chunkPut struct {
	method  interface{}
	params  []reflect.Value
	msgChan chan chunkPutRes
}
type chunkPutRes struct {
	err   error
	state bool

	data interface{}
}

// ChunkInfo
type ChunkInfo struct {
	addr         boson.Address
	storer       storage.StateStorer
	traversal    traversal.Traverser
	route        routetab.RouteTab
	streamer     p2p.Streamer
	logger       logging.Logger
	metrics      metrics
	tt           *timeoutTrigger
	queuesLk     sync.RWMutex
	queues       sync.Map //map[string]*queue
	syncLk       sync.RWMutex
	syncMsg      sync.Map //map[string]chan bool
	ct           *chunkInfoTabNeighbor
	cd           *chunkInfoDiscover
	cp           *chunkPyramid
	cpd          *pendingFinderInfo
	singleflight singleflight.Group
	oracleChain  chain.Resolver
	cs           *chunkInfoSource
}

// New
func New(addr boson.Address, streamer p2p.Streamer, logger logging.Logger, traversal traversal.Traverser, storer storage.StateStorer, route routetab.RouteTab, oracleChain chain.Resolver) *ChunkInfo {
	chunkinfo := &ChunkInfo{
		addr:        addr,
		storer:      storer,
		route:       route,
		metrics:     newMetrics(),
		ct:          newChunkInfoTabNeighbor(),
		cd:          newChunkInfoDiscover(),
		cp:          newChunkPyramid(),
		cpd:         newPendingFinderInfo(),
		tt:          newTimeoutTrigger(),
		streamer:    streamer,
		logger:      logger,
		traversal:   traversal,
		oracleChain: oracleChain,
		cs:          newChunkSource(storer, logger),
	}
	chunkinfo.triggerTimeOut()
	chunkinfo.cleanDiscoverTrigger()
	chunkinfo.chunkPutChanUpdateListen(chunkinfo.cp)
	chunkinfo.chunkPutChanUpdateListen(chunkinfo.cd)
	chunkinfo.chunkPutChanUpdateListen(chunkinfo.ct)
	chunkinfo.chunkPutChanUpdateListen(chunkinfo.cs)
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
	ctx := context.Background()

	if err := ci.chunkPutChanUpdate(ctx, ci.ct, ci.initChunkInfoTabNeighbor).err; err != nil {
		return err
	}
	if err := ci.chunkPutChanUpdate(ctx, ci.cd, ci.initChunkInfoDiscover).err; err != nil {
		return err
	}

	if err := ci.chunkPutChanUpdate(ctx, ci.cs, ci.cs.initChunkInfoSource).err; err != nil {
		return err
	}
	return nil
}

func (ci *ChunkInfo) Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {

	key := fmt.Sprintf("%s%s", rootCid, "chunkinfo")
	v, _, _ := ci.singleflight.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		if ci.cd.isExists(rootCid) {
			return true, nil
		}
		if ci.ct.isDownload(rootCid, ci.addr) {
			return true, nil
		}
		overlays := ci.oracleChain.GetNodesFromCid(rootCid.Bytes())
		if len(overlays) <= 0 {
			return false, nil
		}
		return ci.FindChunkInfo(context.Background(), authInfo, rootCid, overlays), nil
	})
	if v == nil {
		return false
	}
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
		if ci.ct.isExists(rootCid) {
			ticker.Stop()
			ci.syncMsg.Store(rootCid.String(), msgChan)
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
			ci.syncMsg.Delete(rootCid.String())
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
	ci.syncLk.Lock()
	defer ci.syncLk.Unlock()
	if err := ci.pyramidCheck(rootCid, overlay, target); err != nil {
		return err
	}
	return ci.chunkPutChanUpdate(context.Background(), ci.ct, ci.updateNeighborChunkInfo, rootCid, cid, overlay, target).err
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid boson.Address) []*PyramidCidNum {
	return ci.cp.getUnRepeatChunk(rootCid)
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

func (ci *ChunkInfo) DelFile(rootCid boson.Address, del func()) bool {
	ci.syncLk.Lock()
	defer ci.syncLk.Unlock()
	ctx := context.Background()
	ci.CancelFindChunkInfo(rootCid)
	ci.queues.Delete(rootCid.String())
	del()
	if res := ci.chunkPutChanUpdate(ctx, ci.cd, ci.delDiscoverPresence, rootCid).state; !res {
		return false
	}

	if !ci.DelChunkInfoSource(rootCid) {
		return false
	}

	if !ci.chunkPutChanUpdate(ctx, ci.cp, ci.cp.delRootCid, rootCid).state {
		return false
	}

	return ci.chunkPutChanUpdate(ctx, ci.ct, ci.delPresence, rootCid).state
}

func (ci *ChunkInfo) DelDiscover(rootCid boson.Address) {
	ci.syncLk.Lock()
	defer ci.syncLk.Unlock()
	ci.CancelFindChunkInfo(rootCid)
	ci.queues.Delete(rootCid.String())
	ci.chunkPutChanUpdate(context.Background(), ci.cd, ci.delDiscoverPresence, rootCid)
}

//Record every chunk source.
func (ci *ChunkInfo) OnChunkRetrieved(cid, rootCid, sourceOverlay boson.Address) error {
	ci.syncLk.Lock()
	defer ci.syncLk.Unlock()
	if err := ci.pyramidCheck(rootCid, ci.addr, sourceOverlay); err != nil {
		return err
	}
	if err := ci.chunkPutChanUpdate(context.Background(), ci.ct, ci.updateNeighborChunkInfo, rootCid, cid, ci.addr, sourceOverlay).err; err != nil {
		return err
	}
	if err := ci.chunkPutChanUpdate(context.Background(), ci.cs, ci.cs.updatePyramidSource, rootCid, sourceOverlay).err; err != nil {
		return err
	}
	err := ci.chunkPutChanUpdate(context.Background(), ci.cs, ci.UpdateChunkInfoSource, rootCid, sourceOverlay, cid).err
	return err
}

func (ci *ChunkInfo) GetChunkInfoSource(rootCid boson.Address) aurora.ChunkInfoSourceApi {
	return ci.cs.GetChunkInfoSource(rootCid)
}

func (ci *ChunkInfo) DelChunkInfoSource(rootCid boson.Address) bool {
	return ci.chunkPutChanUpdate(context.Background(), ci.cs, ci.cs.DelChunkInfoSource, rootCid).state
}

func (ci *ChunkInfo) chunkPutChanUpdate(ctx context.Context, chunkObj chunkPutEntry, method interface{}, params ...interface{}) (res chunkPutRes) {
	if method == nil {
		res.err = fmt.Errorf("chunkinfo - chunkPutChanUpdate method is nil ")
		res.state = false
		return
	}
	msgCh := make(chan chunkPutRes, 1)

	var pram []reflect.Value
	for _, v := range params {
		pram = append(pram, reflect.ValueOf(v))
	}

	msg := chunkPut{
		method:  method,
		params:  pram,
		msgChan: msgCh,
	}

	chunkObj.getChan() <- msg
	select {
	case res = <-msgCh:
		close(msgCh)
	case <-ctx.Done():
		res.err = fmt.Errorf("chunkinfo chunkPutChanUpdate timeout method = %v", runtime.FuncForPC(reflect.ValueOf(method).Pointer()).Name())
	}

	return res
}

func (ci *ChunkInfo) chunkPutChanUpdateListen(chunkObj chunkPutEntry) {
	update := func(msg chunkPut) {
		chunkObj.setLock()
		defer chunkObj.setUnLock()
		var res chunkPutRes
		var values []reflect.Value
		fn := reflect.ValueOf(msg.method)
		values = fn.Call(msg.params)

		res.err = nil
		res.state = true

		if len(values) > 0 {
			value := values[0]
			switch value.Kind() {
			case reflect.Interface:
				switch value.Interface().(type) {
				case error:
					res.err = value.Interface().(error)
					res.state = false
				default:
					res.data = value.Interface()
				}
			case reflect.Bool:
				res.state = value.Bool()
				if !res.state {
					res.err = fmt.Errorf("chunkinfo - chunkPutChanUpdateListen error,method = %v", runtime.FuncForPC(reflect.ValueOf(msg.method).Pointer()).Name())
				}
			default:
				res.data = value.Interface()
			}
		}
		msg.msgChan <- res
	}
	go func() {
		for msg := range chunkObj.getChan() {
			update(msg)
		}
	}()
}

func (ci *ChunkInfo) pyramidCheck(rootCid, overlay, target boson.Address) error {
	if !ci.cp.isExists(rootCid) {
		if !target.IsZero() && !target.Equal(ci.addr) {
			if err := ci.doFindChunkPyramid(context.Background(), nil, rootCid, target); err != nil {
				return err
			}
		}
		if err := ci.putChunkInfoNeighbor(rootCid, overlay); err != nil {
			return err
		}
	}
	return nil
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
