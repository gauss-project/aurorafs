package chunkinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/storage"
	traversal "github.com/gauss-project/aurorafs/pkg/traversal"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Interface interface {
	FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address)

	GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte

	CancelFindChunkInfo(rootCid boson.Address)

	OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays boson.Address) error

	Init(ctx context.Context, authInfo []byte, rootCid boson.Address) bool

	GetChunkPyramid(rootCid boson.Address) []*boson.Address

	IsDiscover(rootCid boson.Address) bool
}

// ChunkInfo
type ChunkInfo struct {
	storer       storage.StateStorer
	traversal    traversal.Service
	route        routetab.RouteTab
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
func New(streamer p2p.Streamer, logger logging.Logger, traversal traversal.Service, storer storage.StateStorer, oracleUrl string) *ChunkInfo {
	queues := make(map[string]*queue)
	t := time.NewTimer(Time * time.Second)
	return &ChunkInfo{
		storer:    storer,
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
	if err := ci.initChunkPyramid(); err != nil {
		return err
	}
	return nil
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
		errorC            = make(chan error, count)
		first        bool = true
	)
	for {
		if ci.cp.isExists(rootCid) {
			if ci.cd.isExists(rootCid) {
				return true
			}
			if first {
				ci.FindChunkInfo(ctx, authInfo, rootCid, overlays)
				first = false
			}
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
func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	return ci.getChunkInfo(rootCid, cid)
}

// CancelFindChunkInfo
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid boson.Address) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred
func (ci *ChunkInfo) OnChunkTransferred(cid, rootCid boson.Address, overlay boson.Address) error {
	return ci.updateNeighborChunkInfo(rootCid, cid, overlay)
}

func (ci *ChunkInfo) GetChunkPyramid(rootCid boson.Address) []*boson.Address {
	return ci.cp.getChunkCid(rootCid)
}

func (ci *ChunkInfo) IsDiscover(rootCid boson.Address) bool {
	q := ci.getQueue(rootCid.String())

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
