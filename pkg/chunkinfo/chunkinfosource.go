package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
)

func (ci *ChunkInfo) updateSource(rootCid boson.Address, index int64, sourceOverlay boson.Address) error {
	var provider chunkstore.Provider
	var length int64 = index + 1
	if index == 0 {
		size, err := ci.fileInfo.GetFileSize(rootCid)
		if err != nil {
			ci.logger.Errorf("chunkInfo updateService get file size:%w", err)
		} else {
			length = size
		}
	}
	provider.Bit = int(index)
	provider.Len = int(length)
	provider.Overlay = sourceOverlay
	return ci.chunkStore.PutChunk(chunkstore.SOURCE, rootCid, []chunkstore.Provider{provider})
}
