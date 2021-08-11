package traversal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"log"
)

func (s *traversalService) getDataChunksFromManifest(
	ctx context.Context,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
) (addresses [][]byte, err error) {

	isFile, e, _, err := s.checkIsFile(ctx, reference)
	if err != nil {
		return
	}

	if isFile {
		addresses, err = s.getDataChunksAsEntry(ctx, reference, chunkAddressFunc, e)
		return
	}

	err = s.processBytes(ctx, reference, chunkAddressFunc)
	return
}

func (s *traversalService) getDataChunksAsEntry(
	ctx context.Context,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
	e *entry.Entry,
) (addresses [][]byte, err error) {

	bytesReference := e.Reference()

	addresses, err = s.getDataChunks(ctx, bytesReference, chunkAddressFunc)
	if err != nil {
		// possible it was custom JSON bytes, which matches entry JSON
		// but in fact is not file, and does not contain reference to
		// existing address, which is why it was not found in storage
		if !errors.Is(err, storage.ErrNotFound) {
			return
		}
		// ignore
	}

	metadataReference := e.Metadata()

	err = s.processBytes(ctx, metadataReference, chunkAddressFunc)
	if err != nil {
		return
	}

	err = chunkAddressFunc(reference)

	return
}

func (s *traversalService) getDataChunks(
	ctx context.Context,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
) (addresses [][]byte, err error) {
	j, _, err := joiner.New(ctx, s.storer, reference)
	if err != nil {
		err = fmt.Errorf("traversal: joiner: %s: %w", reference, err)
		return
	}

	j.SetSaveDataChunks()

	err = j.IterateChunkAddresses(chunkAddressFunc)
	if err != nil {
		err = fmt.Errorf("traversal: iterate chunks: %s: %w", reference, err)
		return
	}
	addresses = j.GetDataChunks()

	return
}

func (s *traversalService) getChunkBytes(
	ctx context.Context,
	reference boson.Address,
	trieData map[string][]byte,
) error {
	j, _, err := joiner.New(ctx, s.storer, reference)
	if err != nil {
		return fmt.Errorf("traversal: joiner: %s: %w", reference, err)
	}

	trieData[reference.String()] = j.GetRootData()

	return nil
}

func (s *traversalService) getChunkAsEntry(
	ctx context.Context,
	e *entry.Entry,
	trieData map[string][]byte,
) (err error) {

	bytesReference := e.Reference()
	err = s.getChunkBytes(ctx, bytesReference, trieData)
	if err != nil {
		// possible it was custom JSON bytes, which matches entry JSON
		// but in fact is not file, and does not contain reference to
		// existing address, which is why it was not found in storage
		if !errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		// ignore
	}

	metadataReference := e.Metadata()
	err = s.getChunkBytes(ctx, metadataReference, trieData)
	if err != nil {
		return
	}

	return nil
}

func (s *traversalService) getChunkFromManifest(
	ctx context.Context,
	reference boson.Address,
	trieData map[string][]byte,
) error {

	isFile, e, _, err := s.getChunkAsFile(ctx, reference, trieData)
	if err != nil {
		return err
	}

	if isFile {
		return s.getChunkAsEntry(ctx, e, trieData)
	}

	return s.getChunkBytes(ctx, reference, trieData)
}

// checks if the content is file.
func (s *traversalService) getChunkAsFile(
	ctx context.Context,
	reference boson.Address,
	trieData map[string][]byte,
) (isFile bool, e *entry.Entry, metadata *entry.Metadata, err error) {

	var (
		j    file.Joiner
		span int64
	)
	j, span, err = joiner.New(ctx, s.storer, reference)
	if err != nil {
		err = fmt.Errorf("traversal: joiner: %s: %w", reference, err)
		return
	}

	maybeIsFile := entry.CanUnmarshal(span)

	if maybeIsFile {
		buf := bytes.NewBuffer(nil)
		_, err = file.JoinReadAll(ctx, j, buf)
		if err != nil {
			err = fmt.Errorf("traversal: read entry: %s: %w", reference, err)
			return
		}

		trieData[reference.String()] = buf.Bytes()

		e = &entry.Entry{}
		err = e.UnmarshalBinary(buf.Bytes())
		if err != nil {
			err = fmt.Errorf("traversal: unmarshal entry: %s: %w", reference, err)
			return
		}

		// address sizes must match
		if len(reference.Bytes()) != len(e.Reference().Bytes()) {
			return
		}

		// NOTE: any bytes will unmarshall to addresses; we need to check metadata

		// read metadata
		j, _, err = joiner.New(ctx, s.storer, e.Metadata())
		if err != nil {
			// ignore
			err = nil
			return
		}

		buf = bytes.NewBuffer(nil)
		_, err = file.JoinReadAll(ctx, j, buf)
		if err != nil {
			err = fmt.Errorf("traversal: read metadata: %s: %w", reference, err)
			return
		}

		metadata = &entry.Metadata{}

		dec := json.NewDecoder(buf)
		dec.DisallowUnknownFields()
		err = dec.Decode(metadata)
		if err != nil {
			// may not be metadata JSON
			err = nil
			return
		}
		isFile = true
	}

	return
}

// GetTrieData get trie data by reference
func (s *traversalService) GetTrieData(ctx context.Context, reference boson.Address) (trieData map[string][]byte, err error) {
	var (
		isFile, isManifest bool
		m                  manifest.Interface
		e                  *entry.Entry
		metadata           *entry.Metadata
	)
	trieData = make(map[string][]byte)
	isFile, e, metadata, err = s.getChunkAsFile(ctx, reference, trieData)
	if err != nil {
		return
	}
	if !isFile {
		err = ErrInvalidType
		return
	}
	isManifest, m, err = s.checkIsManifest(ctx, reference, e, metadata)
	if err != nil {
		return
	}
	if isManifest {
		err = m.IterateAddresses(ctx, func(manifestNodeAddr boson.Address) error {
			return s.getChunkFromManifest(ctx, manifestNodeAddr, trieData)
		})
		if err != nil {
			err = fmt.Errorf("traversal: iterate chunks: %s: %w", reference, err)
			return
		}

		metadataReference := e.Metadata()

		err = s.getChunkBytes(ctx, metadataReference, trieData)
	} else {
		err = s.getChunkAsEntry(ctx, e, trieData)
	}
	return
}

// CheckTrieData check the trie data
func (s *traversalService) CheckTrieData(ctx context.Context, reference boson.Address, trieData map[string][]byte) (dataChunks [][][]byte, err error) {
	for k, v := range trieData {
		var addr boson.Address
		addr, err = boson.ParseHexAddress(k)
		if err != nil {
			return
		}
		_, err = s.storer.Put(ctx, storage.ModePutUpload, boson.NewChunk(addr, v))
		if err != nil {
			return
		}
	}
	var (
		isFile, isManifest bool
		m                  manifest.Interface
		e                  *entry.Entry
		metadata           *entry.Metadata
		chunkAddressFunc   = func(address boson.Address) error { return nil }
	)
	isFile, e, metadata, err = s.checkIsFile(ctx, reference)
	if err != nil {
		return
	}
	if !isFile {
		err = ErrInvalidType
		return
	}

	isManifest, m, err = s.checkIsManifest(ctx, reference, e, metadata)
	if err != nil {
		return
	}

	if isManifest {
		// process as manifest

		err = m.IterateAddresses(ctx, func(manifestNodeAddr boson.Address) error {
			log.Println("manifestNodeAddr: ", manifestNodeAddr)
			addresses, err1 := s.getDataChunksFromManifest(ctx, manifestNodeAddr, chunkAddressFunc)
			if err1 != nil {
				return err1
			}

			dataChunks = append(dataChunks, addresses)
			return nil
		})
		if err != nil {
			err = fmt.Errorf("traversal: iterate chunks: %s: %w", reference, err)
			return
		}

		metadataReference := e.Metadata()

		err = s.processBytes(ctx, metadataReference, chunkAddressFunc)

	} else {
		addresses, er := s.getDataChunksAsEntry(ctx, reference, chunkAddressFunc, e)
		if er != nil {
			err = er
			return
		}
		dataChunks = append(dataChunks, addresses)
	}
	return
}
