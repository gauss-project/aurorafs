package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/aurora"
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

func (ci *ChunkInfo) updateService(rootCid boson.Address, bit int, overlay boson.Address) error {
	has, err := ci.chunkStore.HasChunk(chunkstore.SERVICE, rootCid, overlay)
	if err != nil {
		return err
	}

	var provider chunkstore.Provider
	provider.Len = bit
	provider.Bit = bit
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
	// TODO
	// if we get the first three chunk of a file,we need to add it into the fileStore
	// and the update the correct of bitLen
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

func (ci *ChunkInfo) getService(rootCid boson.Address) ([]aurora.ChunkInfoOverlay, error) {
	res := make([]aurora.ChunkInfoOverlay, 0)
	consumerList, err := ci.chunkStore.GetChunk(chunkstore.SERVICE, rootCid)
	if err != nil {
		return nil, err
	}
	for _, c := range consumerList {
		bv := aurora.BitVectorApi{Len: c.Len, B: c.B}
		cio := aurora.ChunkInfoOverlay{Overlay: c.Overlay.String(), Bit: bv}
		res = append(res, cio)
	}
	return res, nil
}

func (ci *ChunkInfo) removeService(rootCid boson.Address) error {
	err := ci.chunkStore.DeleteAllChunk(chunkstore.SERVICE, rootCid)
	if err != nil {
		return err
	}
	go ci.PublishRootCidStatus(RootCidStatusEven{
		RootCid: rootCid,
		Status:  RootCid_DEL,
	})
	return nil
}
