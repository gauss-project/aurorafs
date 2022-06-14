package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
)

func (ci *ChunkInfo) updateSource(rootCid boson.Address, bit int, sourceOverlay boson.Address) error {
	var provider chunkstore.Provider
	provider.Bit = bit
	provider.Len = bit
	provider.Overlay = sourceOverlay
	return ci.chunkStore.PutChunk(chunkstore.SOURCE, rootCid, []chunkstore.Provider{provider})
}

func (ci *ChunkInfo) getSource(rootCid boson.Address) (sourceResp aurora.ChunkInfoSourceApi, err error) {
	consumerList, err := ci.chunkStore.GetChunk(chunkstore.SOURCE, rootCid)
	if err != nil {
		return
	}

	for _, c := range consumerList {
		chunkBit := aurora.BitVectorApi{
			Len: c.Len,
			B:   c.B,
		}
		source := aurora.ChunkSourceApi{
			Overlay:  c.Overlay.String(),
			ChunkBit: chunkBit,
		}
		sourceResp.ChunkSource = append(sourceResp.ChunkSource, source)
	}
	return
}

func (ci *ChunkInfo) removeSource(rootCid boson.Address) error {
	return ci.chunkStore.DeleteAllChunk(chunkstore.SOURCE, rootCid)
}
