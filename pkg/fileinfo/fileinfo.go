package fileinfo

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/localstore"
	"github.com/gauss-project/aurorafs/pkg/localstore/chunkstore"
	"github.com/gauss-project/aurorafs/pkg/localstore/filestore"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/resolver"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

type FileView struct {
	filestore.FileView
	BvLen int
	Bv    []byte
}

type Interface interface {
	GetFileList(page filestore.Page, filter []filestore.Filter, sort filestore.Sort) []FileView
	GetFileSize(rootCid boson.Address) (int64, error)
	ManifestView(ctx context.Context, nameOrHex string, pathVar string, depth int) (*ManifestNode, error)
	AddFile(rootCid boson.Address) error
	DeleteFile(rootCid boson.Address) error
	PinFile(rootCid boson.Address, pinned bool) error
	RegisterFile(rootCid boson.Address, registered bool) error
	GetChunkInfoDiscoverOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay
	GetChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay
	GetChunkInfoSource(rootCid boson.Address) aurora.ChunkInfoSourceApi
}

type FileInfo struct {
	addr       boson.Address
	localStore *localstore.DB
	logger     logging.Logger
	resolver   resolver.Interface
}

func New(addr boson.Address, db *localstore.DB, logger logging.Logger, resolver resolver.Interface) Interface {
	return &FileInfo{
		addr:       addr,
		localStore: db,
		logger:     logger,
		resolver:   resolver,
	}
}

func (f *FileInfo) GetFileSize(rootCid boson.Address) (int64, error) {
	ctx := context.TODO()
	chunk, err := f.localStore.Get(ctx, storage.ModeGetRequest, rootCid, 0)
	if err != nil {
		return 0, err
	}
	var chunkData = chunk.Data()
	span := int64(binary.LittleEndian.Uint64(chunkData[:boson.SpanSize]))
	size := chunkLen(span)
	return size, nil
}

func (f *FileInfo) GetFileList(page filestore.Page, filter []filestore.Filter, sort filestore.Sort) []FileView {
	list := f.localStore.GetListFile(page, filter, sort)
	fileList := make([]FileView, 0, len(list))
	for index := 0; index < len(list); index++ {
		fv := FileView{
			FileView: list[index],
		}
		consumerList, err := f.localStore.GetChunk(chunkstore.SERVICE, list[index].RootCid)
		if err != nil {
			f.logger.Errorf("fileInfo GetFileList:%w", err)
		} else {
			for cIndex := 0; cIndex < len(consumerList); cIndex++ {
				if consumerList[cIndex].Overlay.Equal(f.addr) {
					fv.Bv = consumerList[cIndex].B
					fv.BvLen = consumerList[cIndex].Len
					break
				}
			}
		}
		fileList = append(fileList, fv)
	}
	return fileList
}

func (f *FileInfo) AddFile(rootCid boson.Address) error {
	if f.localStore.HasFile(rootCid) {
		return nil
	}
	manifest, err := f.ManifestView(context.TODO(), rootCid.String(), defaultPathVar, defaultDepth)
	if err != nil {
		return fmt.Errorf("fileStore get manifest:%s", err.Error())
	}
	fileInfo := filestore.FileView{
		RootCid:    rootCid,
		Pinned:     false,
		Registered: false,
		Size:       int(manifest.Size),
		Type:       manifest.Type,
		Name:       manifest.Name,
		Extension:  manifest.Extension,
		MimeType:   manifest.MimeType,
	}
	if f.localStore.HasFile(rootCid) {

	}
	err = f.localStore.PutFile(fileInfo)
	if err != nil {
		return fmt.Errorf("fileStore put new fileinfo %s:%s", rootCid.String(), err.Error())
	}
	return nil
}

func (f *FileInfo) DeleteFile(rootCid boson.Address) error {
	return f.localStore.DeleteFile(rootCid)
}

func (f *FileInfo) PinFile(rootCid boson.Address, pinned bool) error {
	file, ok := f.localStore.GetFile(rootCid)
	if !ok {
		return fmt.Errorf("fileStore update fileinfo %s:fileinfo not found", rootCid.String())
	}
	file.Pinned = pinned
	err := f.localStore.PutFile(file)
	if err != nil {
		return fmt.Errorf("fileStore put new fileinfo %s:%s", rootCid.String(), err.Error())
	}
	return nil
}

func (f *FileInfo) RegisterFile(rootCid boson.Address, registered bool) error {
	file, ok := f.localStore.GetFile(rootCid)
	if !ok {
		return fmt.Errorf("fileStore update fileinfo %s:fileinfo not found", rootCid.String())
	}
	file.Registered = registered
	err := f.localStore.PutFile(file)
	if err != nil {
		return fmt.Errorf("fileStore put new fileinfo %s:%s", rootCid.String(), err.Error())
	}
	return nil
}

func (f *FileInfo) GetChunkInfoDiscoverOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	res := make([]aurora.ChunkInfoOverlay, 0)
	consumerList, err := f.localStore.GetChunk(chunkstore.DISCOVER, rootCid)
	if err != nil {
		f.logger.Errorf("fileInfo GetChunkInfoDiscoverOverlays:%w", err)
		return res
	}
	for _, c := range consumerList {
		bv := aurora.BitVectorApi{B: c.B, Len: c.Len}
		cio := aurora.ChunkInfoOverlay{Overlay: c.Overlay.String(), Bit: bv}
		res = append(res, cio)
	}
	return res
}

func (f *FileInfo) GetChunkInfoServerOverlays(rootCid boson.Address) []aurora.ChunkInfoOverlay {
	res := make([]aurora.ChunkInfoOverlay, 0)
	consumerList, err := f.localStore.GetChunk(chunkstore.SERVICE, rootCid)
	if err != nil {
		f.logger.Errorf("fileInfo GetChunkInfoServerOverlays:%w", err)
		return res
	}
	for _, c := range consumerList {
		bv := aurora.BitVectorApi{Len: c.Len, B: c.B}
		cio := aurora.ChunkInfoOverlay{Overlay: c.Overlay.String(), Bit: bv}
		res = append(res, cio)
	}
	return res
}

func (f *FileInfo) GetChunkInfoSource(rootCid boson.Address) aurora.ChunkInfoSourceApi {
	var res aurora.ChunkInfoSourceApi
	consumerList, err := f.localStore.GetChunk(chunkstore.SOURCE, rootCid)
	if err != nil {
		f.logger.Errorf("fileInfo GetChunkInfoSource:%w", err)
		return res
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
		res.ChunkSource = append(res.ChunkSource, source)
	}
	return res
}

func chunkLen(span int64) int64 {
	count := span / boson.ChunkSize
	count1 := span % boson.ChunkSize
	if count1 > 0 {
		count1 = 1
	}
	count += count1
	if count > boson.Branches {
		count += chunkLen1(count)
	} else {
		count++
	}
	return count
}

func chunkLen1(count int64) int64 {
	count1 := count / boson.Branches
	if count1 == 0 {
		return 1
	}
	count2 := count % boson.Branches
	count = count2 + count1
	if count == 1 {
		return count
	}
	count = chunkLen1(count)
	return count1 + count
}
