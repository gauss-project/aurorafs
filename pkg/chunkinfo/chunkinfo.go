package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	traversal "github.com/gauss-project/aurorafs/pkg/traversal"
	"sync"
	"time"
)

type Interface interface {
	FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address)

	GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte

	CancelFindChunkInfo(rootCid boson.Address)

	OnChunkTransferred(cid boson.Address, rootCid boson.Address, overlays boson.Address)
}

// ChunkInfo
type ChunkInfo struct {
	// store
	traversal traversal.Service
	streamer  p2p.Streamer
	logger    logging.Logger
	t         *time.Timer
	tt        *timeoutTrigger
	queuesLk  sync.RWMutex
	queues    map[string]*queue
	ct        *chunkInfoTabNeighbor
	cd        *chunkInfoDiscover
	cp        *chunkPyramid
	cpd       *pendingFinderInfo
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

// FindChunkInfo
func (ci *ChunkInfo) FindChunkInfo(ctx context.Context, authInfo []byte, rootCid boson.Address, overlays []boson.Address) {
	go ci.triggerTimeOut()
	ci.cpd.updatePendingFinder(rootCid)
	if ci.cd.isExists(rootCid) {
		for _, overlay := range overlays {
			ci.getQueue(rootCid.String()).push(UnPull, overlay.Bytes())
		}
		ci.doFindChunkInfo(ctx, authInfo, rootCid)
	} else {
		ci.newQueue(rootCid.String())
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
