package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
)

type RootCidStatusEven struct {
	RootCid boson.Address
	Status  RootCidStatus
}

type RootCidStatus = int

const (
	RootCid_DEL RootCidStatus = iota
	RootCid_ADD
)

func (ci *ChunkInfo) isDownload(rootCid, overlay boson.Address) bool {
	consumerList, err := ci.chunkStore.GetChunk(chunkstore.SERVICE, rootCid)
	if err != nil {
		ci.logger.Errorf("chunkInfo isDownload:%w", err)
		return false
	}
	for _, c := range consumerList {
		if c.Overlay.Equal(overlay) {
			bv, err := bitvector.NewFromBytes(c.B, c.Len)
			if err != nil {
				ci.logger.Errorf("chunkInfo isDownload construct bitVector:%w", err)
				return false
			}
			return bv.Equals()
		}
	}
	return false
}

func (ci *ChunkInfo) updateService(rootCid boson.Address, index int64, overlay boson.Address) error {
	has, err := ci.chunkStore.HasChunk(chunkstore.SERVICE, rootCid, overlay)
	if err != nil {
		return err
	}
	var length int64 = index + 1
	if index == 0 {
		size, err := ci.fileInfo.GetFileSize(rootCid)
		if err != nil {
			ci.logger.Errorf("chunkInfo updateService get file size:%w", err)
		} else {
			length = size
		}
	}
	var provider chunkstore.Provider
	provider.Len = int(length)
	provider.Bit = int(index)
	provider.Overlay = overlay
	err = ci.chunkStore.PutChunk(chunkstore.SERVICE, rootCid, []chunkstore.Provider{provider})
	if err != nil {
		return err
	}

	var consumer chunkstore.Consumer
	consumerList, err := ci.chunkStore.GetChunk(chunkstore.SERVICE, rootCid)
	if err != nil {
		return err
	}

	for i := range consumerList {
		if consumerList[i].Overlay.Equal(overlay) {
			consumer = consumerList[i]
			break
		}
	}
	if index == 0 || index == 1 || index == 2 {
		bitV, _ := bitvector.NewFromBytes(consumer.B, consumer.Len)
		if bitV.Get(0) && bitV.Get(1) && bitV.Get(2) {
			err = ci.fileInfo.AddFile(rootCid)
			if err != nil {
				ci.logger.Errorf("chunkInfo updateService AddFile:%w", err)
			}
		}
	}
	if !has {
		if overlay.Equal(ci.addr) {
			go ci.PublishRootCidStatus(RootCidStatusEven{
				RootCid: rootCid,
				Status:  RootCid_ADD,
			})
		}
	} else {
		bv := BitVector{B: consumer.B, Len: consumer.Len}
		if overlay.Equal(ci.addr) {
			go ci.PublishDownloadProgress(rootCid, BitVectorInfo{
				RootCid:   rootCid,
				Bitvector: bv,
			})
		} else {
			go ci.PublishRetrievalProgress(rootCid, BitVectorInfo{
				RootCid:   rootCid,
				Overlay:   overlay,
				Bitvector: bv,
			})
		}
	}

	return nil
}
