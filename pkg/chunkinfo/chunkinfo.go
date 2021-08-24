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

	OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays boson.Address)

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
}

// New
func New(streamer p2p.Streamer, logger logging.Logger, traversal traversal.Service) *ChunkInfo {
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
	}
}

const (
	chunkInfoRetryIntervalDuration = 5 * time.Second
	oracleUrl                      = "http://127.0.0.1:8000/api/v1.0/rcid"
)

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
	v, _, _ := ci.singleflight.Do(rootCid.String(), func() (interface{}, error) {
		r, err := http.Get(fmt.Sprintf("%s/%s", oracleUrl, rootCid.String()))
		if err != nil {
			return false, nil
		}
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return false, nil
		}
		var resp Response
		if err := json.Unmarshal(data, &resp); err != nil {
			return false, nil
		}
		if r.StatusCode != http.StatusOK {
			ci.logger.Errorf("expected %d response, got %d", http.StatusOK, r.StatusCode)
			return false, nil
		}
		if resp.StatusCode != http.StatusOK {
			ci.logger.Errorf("expected %d response, got %d", http.StatusOK, r.StatusCode)
			return false, nil
		}
		addrs := resp.Body.Addresses
		count := len(addrs)
		if count <= 0 {
			return false, nil
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

		for {

			if peerAttempt < count {
				if ci.cd.isExists(rootCid) {
					ci.FindChunkInfo(ctx, authInfo, rootCid, overlays[peerAttempt:])
					return true, nil
				}
				if ci.getQueue(rootCid.String()) == nil {
					ci.newQueue(rootCid.String())
				}
				cpReq := ci.cp.createChunkPyramidReq(rootCid)
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
				return false, nil
			}
			if peersResults >= count {
				return false, nil
			}
		}
	})
	return v.(bool)
}

// FindChunkInfo
func (ci *ChunkInfo) FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	go ci.triggerTimeOut()
	ci.cpd.updatePendingFinder(rootCid)
	if ci.getQueue(rootCid.String()) == nil {
		ci.newQueue(rootCid.String())
	}
	if ci.cd.isExists(rootCid) {
		for _, overlay := range overlays {
			ci.getQueue(rootCid.String()).push(UnPull, overlay.Bytes())
		}
		ci.doFindChunkInfo(ctx, authInfo, rootCid)
	} else {
		for _, overlay := range overlays {
			ci.getQueue(rootCid.String()).push(UnPull, overlay.Bytes())
		}
		ci.doFindChunkPyramid(ctx, authInfo, rootCid, overlays)
	}
}

// GetChunkInfo
func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	return ci.cd.getChunkInfo(rootCid, cid)
}

// CancelFindChunkInfo
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred
func (ci *ChunkInfo) OnChunkTransferred(cid, rootCid boson.Address, overlay boson.Address) {
	ci.ct.updateNeighborChunkInfo(rootCid, cid, overlay)
}
