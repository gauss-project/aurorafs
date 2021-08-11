package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"time"
)

type Interface interface {
	FindChunkInfo(authInfo []byte, rootCid string, nodes []string)

	GetChunkInfo(rootCid string, cid string) []string

	GetChunkPyramid(rootCid string) map[string]map[string]uint

	CancelFindChunkInfo(rootCid string)

	OnChunkTransferred(cid string, rootCid string, node string)
}

// ChunkInfo
type ChunkInfo struct {
	// store
	streamer p2p.Streamer
	tracer   *tracing.Tracer
	logger   logging.Logger
	t        *time.Timer
	tt       *timeoutTrigger
	queues   map[string]*queue
	ct       *chunkInfoTabNeighbor
	cd       *chunkInfoDiscover
	cp       *chunkPyramid
	cpd      *pendingFinderInfo
}

// New
func New(streamer p2p.Streamer, logger logging.Logger, tracer *tracing.Tracer) *ChunkInfo {
	// message new
	cd := &chunkInfoDiscover{presence: make(map[string]map[string][]string)}
	ct := &chunkInfoTabNeighbor{presence: make(map[string][]string)}
	cp := &chunkPyramid{pyramid: map[string]map[string]uint{}}
	cpd := &pendingFinderInfo{finder: make(map[string]struct{})}
	tt := &timeoutTrigger{trigger: make(map[string]int64)}
	queues := make(map[string]*queue)
	t := time.NewTimer(Time * time.Second)
	return &ChunkInfo{ct: ct,
		cd:       cd,
		cp:       cp,
		cpd:      cpd,
		queues:   queues,
		t:        t,
		tt:       tt,
		streamer: streamer,
		logger:   logger,
		tracer:   tracer,
	}
}

// FindChunkInfo
func (ci *ChunkInfo) FindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	go ci.triggerTimeOut()
	ci.cpd.updatePendingFinder(rootCid)
	if ci.cd.isExists(rootCid) {
		for _, n := range nodes {
			ci.queues[rootCid].push(Pulling, n)
		}
		ci.doFindChunkInfo(authInfo, rootCid)
	} else {
		ci.newQueue(rootCid)
		for _, n := range nodes {
			ci.getQueue(rootCid).push(Pulling, n)
		}
		ci.doFindChunkPyramid(authInfo, rootCid, nodes)
	}
}

// GetChunkInfo
func (ci *ChunkInfo) GetChunkInfo(rootCid string, cid string) []string {
	return ci.cd.getChunkInfo(rootCid, cid)
}

// GetChunkPyramid
func (ci *ChunkInfo) GetChunkPyramid(rootCid string) map[string]map[string]uint {
	return ci.cp.getChunkPyramid(rootCid)
}

// CancelFindChunkInfo
func (ci *ChunkInfo) CancelFindChunkInfo(rootCid string) {
	ci.cpd.cancelPendingFinder(rootCid)
}

// OnChunkTransferred
func (ci *ChunkInfo) OnChunkTransferred(cid string, rootCid string, node string) {
	ci.ct.updateNeighborChunkInfo(rootCid, cid, node)
}
