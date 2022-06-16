package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
)

func (ci *ChunkInfo) updateSource(rootCid boson.Address, bit int, length int, sourceOverlay boson.Address) error {
	var provider chunkstore.Provider
	provider.Bit = bit
	provider.Len = length
	provider.Overlay = sourceOverlay
	return ci.chunkStore.PutChunk(chunkstore.SOURCE, rootCid, []chunkstore.Provider{provider})
}
