package chunkinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	traversal "github.com/gauss-project/aurorafs/pkg/traversal"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"sync"
	"time"
)

type Interface interface {
	FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address)

	GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte

	CancelFindChunkInfo(rootCid boson.Address)

	OnChunkTransferred(ctx context.Context, cid boson.Address, rootCid boson.Address, overlays boson.Address)

	Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool
}

// ChunkInfo
type ChunkInfo struct {
	// store
	traversal    traversal.Service
	streamer     p2p.Streamer
	logger       logging.Logger
	t            *time.Timer
	tt           *timeoutTrigger
	queuesLk     sync.RWMutex
	queues       map[string]*queue
	ct           *chunkInfoTabNeighbor
	cd           *chunkInfoDiscover
	cp           *chunkPyramid
	cpd          *pendingFinderInfo
	singleflight singleflight.Group
	oracleUrl    string
}

// New
func New(streamer p2p.Streamer, logger logging.Logger, traversal traversal.Service, oracleUrl string) *ChunkInfo {
	queues := make(map[string]*queue)
	t := time.NewTimer(Time * time.Second)
	return &ChunkInfo{
		ct:        newChunkInfoTabNeighbor(),
		cd:        newChunkInfoDiscover(),
		cp:        newChunkPyramid(),
		cpd:       newPendingFinderInfo(),
		queues:    queues,
		t:         t,
		tt:        newTimeoutTrigger(),
		streamer:  streamer,
		logger:    logger,
		traversal: traversal,
		oracleUrl: oracleUrl,
	}
}

const chunkInfoRetryIntervalDuration = 5 * time.Second

type Response struct {
	StatusCode int             `json:"code"`
	Message    string          `json:"msg"`
	Body       RootCIDResponse `json:"data"`
}

type RootCIDResponse struct {
	RootCID   string   `json:"rootcid"`
	Addresses []string `json:"addresses"`
}

func (ci *ChunkInfo) Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool {
	if ci.ct.isExists(rootCid) {
		return true
	}

	r, err := http.Get(fmt.Sprintf("http://%s/api/v1.0/rcid/%s", ci.oracleUrl, rootCid.String()))
	if err != nil {
		return false
	}
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return false
	}
	var resp Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return false
	}
	if r.StatusCode != http.StatusOK {
		ci.logger.Errorf("expected %d response, got %d", http.StatusOK, r.StatusCode)
		return false
	}
	if resp.StatusCode != 400 {
		ci.logger.Errorf("expected %d response, got %d", http.StatusOK, r.StatusCode)
		return false
	}
	addrs := resp.Body.Addresses
	count := len(addrs)
	if count <= 0 {
		return false
	}
	overlays := make([]boson.Address, 0, count)
	for _, addr := range addrs {
		a, _ := boson.ParseHexAddress(addr)
		overlays = append(overlays, a)
	}

	ticker := time.NewTicker(chunkInfoRetryIntervalDuration)
	defer ticker.Stop()

	var (
		peerAttempt  int
		peersResults int
		errorC       = make(chan error, count)
	)
	// todo 修改逻辑
	for {
		if ci.cd.isExists(rootCid) {
			if len(overlays) > 1 {
				ci.FindChunkInfo(ctx, authInfo, rootCid, overlays[peerAttempt:])
			}
			return true
		}
		if peerAttempt < count {
			if ci.getQueue(rootCid.String()) == nil {
				ci.newQueue(rootCid.String())
				ci.getQueue(rootCid.String()).push(UnPull, overlays[peerAttempt].Bytes())
			}
			cpReq := ci.cp.createChunkPyramidReq(rootCid)
			// todo 调用获取树
			if err := ci.sendData(ctx, overlays[peerAttempt], streamPyramidReqName, cpReq); err != nil {
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
		}
		if peersResults >= count {
			return false

		}
	}
}

// FindChunkInfo
func (ci *ChunkInfo) FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	go ci.triggerTimeOut()
	ci.cpd.updatePendingFinder(rootCid)
	if ci.getQueue(rootCid.String()) == nil {
		ci.newQueue(rootCid.String())
	}
	// todo 修改逻辑
	for _, overlay := range overlays {
		if ci.getQueue(rootCid.String()).isExists(Pulled, overlay.Bytes()) || ci.getQueue(rootCid.String()).isExists(Pulling, overlay.Bytes()) ||
			ci.getQueue(rootCid.String()).isExists(UnPull, overlay.Bytes()) {
			continue
		}
		ci.getQueue(rootCid.String()).push(UnPull, overlay.Bytes())
	}
	if ci.cd.isExists(rootCid) {
		ci.doFindChunkInfo(ctx, authInfo, rootCid)
	} else {
		ci.doFindChunkPyramid(ctx, authInfo, rootCid, overlays)
	}
}

// GetChunkInfo
func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	return ci.getChunkInfo(rootCid, cid)
}

// CancelFindChunkInfo
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred
func (ci *ChunkInfo) OnChunkTransferred(ctx context.Context, cid, rootCid boson.Address, overlay boson.Address) {
	ci.updateNeighborChunkInfo(ctx, rootCid, cid, overlay)
}
