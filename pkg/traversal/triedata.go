package traversal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/bmt"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

func (s *traversalService) getDataChunksFromManifest(
	ctx context.Context,
	getter storage.Getter,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
) (addresses [][]byte, err error) {

	isFile, e, _, err := s.checkIsFileWithCustomStore(ctx, getter, reference)
	if err != nil {
		return
	}

	if isFile {
		addresses, err = s.getDataChunksAsEntry(ctx, getter, reference, chunkAddressFunc, e)
		return
	}

	err = s.processBytesWithCustomStore(ctx, getter, reference, chunkAddressFunc)
	return
}

func (s *traversalService) getDataChunksAsEntry(
	ctx context.Context,
	getter storage.Getter,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
	e *entry.Entry,
) (addresses [][]byte, err error) {

	bytesReference := e.Reference()

	addresses, err = s.getDataChunks(ctx, getter, bytesReference, chunkAddressFunc)
	if err != nil {
		return
	}

	metadataReference := e.Metadata()

	err = s.processBytesWithCustomStore(ctx, getter, metadataReference, chunkAddressFunc)
	if err != nil {
		return
	}

	err = chunkAddressFunc(reference)

	return
}

func (s *traversalService) getDataChunks(
	ctx context.Context,
	getter storage.Getter,
	reference boson.Address,
	chunkAddressFunc boson.AddressIterFunc,
) (addresses [][]byte, err error) {
	j, _, err := joiner.New(ctx, getter, reference)
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

func dataWithSpan(data []byte, size uint64) []byte {
	spanData := make([]byte, len(data)+8)
	if size == 0 {
		size = uint64(len(data))
	}
	binary.LittleEndian.PutUint64(spanData[:8], size)
	copy(spanData[8:], data)
	return spanData
}

func (s *traversalService) getChunkBytes(
	ctx context.Context,
	reference boson.Address,
	trieData map[string][]byte,
	isMetadata bool,
) error {
	j, span, err := joiner.New(ctx, s.storer, reference)
	if err != nil {
		return fmt.Errorf("traversal: joiner: %s: %w", reference, err)
	}

	// for one chunk, it should save file chunk for known file size.
	if isMetadata || span >= int64(len(j.GetRootData())) {
		trieData[reference.String()] = dataWithSpan(j.GetRootData(), uint64(span))
	}

	j.SetSaveIntChunks(trieData)
	j.IterateChunkAddresses(func(addr boson.Address) error { return nil })

	return nil
}

func (s *traversalService) getChunkAsEntry(
	ctx context.Context,
	e *entry.Entry,
	trieData map[string][]byte,
) (err error) {

	bytesReference := e.Reference()
	err = s.getChunkBytes(ctx, bytesReference, trieData, false)
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
	err = s.getChunkBytes(ctx, metadataReference, trieData, true)
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

	return s.getChunkBytes(ctx, reference, trieData, false)
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

	// ref hash -> intermediate chunk data
	trieData[reference.String()] = dataWithSpan(j.GetRootData(), uint64(span))

	maybeIsFile := entry.CanUnmarshal(span)
	if maybeIsFile {
		buf := bytes.NewBuffer(nil)
		_, err = file.JoinReadAll(ctx, j, buf)
		if err != nil {
			err = fmt.Errorf("traversal: read entry: %s: %w", reference, err)
			return
		}

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

		err = s.getChunkBytes(ctx, metadataReference, trieData, true)
	} else {
		err = s.getChunkAsEntry(ctx, e, trieData)
	}
	return
}

type pyramid struct {
	data map[string][]byte
	seen map[string]struct{}
	mu   sync.Mutex
}

func newPyramid(trieData map[string][]byte) *pyramid {
	return &pyramid{
		data: trieData,
		seen: make(map[string]struct{}),
	}
}

func (p *pyramid) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	addrStr := addr.String()
	val, exists := p.data[addrStr]
	if !exists {
		err = storage.ErrNotFound
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.seen[addrStr]; !exists {
		p.seen[addrStr] = struct{}{}
	}

	ch = boson.NewChunk(addr, val)
	return
}

func (p *pyramid) Put(_ context.Context, _ storage.ModePut, _ ...boson.Chunk) ([]bool, error) {
	panic("not implemented")
}

func (p *pyramid) GetMulti(_ context.Context, _ storage.ModeGet, _ ...boson.Address) ([]boson.Chunk, error) {
	panic("not implemented")
}

func (p *pyramid) Set(_ context.Context, _ storage.ModeSet, _ ...boson.Address) error {
	panic("not implemented")
}

func (p *pyramid) Has(_ context.Context, hasMode storage.ModeHas, _ boson.Address) (bool, error) {
	panic("not implemented")
}

func (p *pyramid) HasMulti(_ context.Context, hasMode storage.ModeHas, _ ...boson.Address) ([]bool, error) {
	panic("not implemented")
}

func (p *pyramid) Close() error {
	return nil
}

type nopChainWriter struct{}

func (n *nopChainWriter) ChainWrite(_ *pipeline.PipeWriteArgs) error { return nil }
func (n *nopChainWriter) Sum() ([]byte, error)                       { return nil, nil }

var ErrInvalidTrie = errors.New("traversal: invalid trie data")

// CheckTrieData check the trie data, return leaf node chunk hash array.
func (s *traversalService) CheckTrieData(ctx context.Context, reference boson.Address, trieData map[string][]byte) (dataChunks [][][]byte, err error) {
	var (
		e                  *entry.Entry
		addr               boson.Address
		isFile, isManifest bool
		m                  manifest.Interface
		metadata           *entry.Metadata
	)
	if _, exists := trieData[reference.String()]; !exists {
		return nil, fmt.Errorf("invalid trie data without reference %s\n", reference)
	}
	// verify each data could be sum up the correct hash.
	bmtWriter := bmt.NewBmtWriter(&nopChainWriter{})
	for expectedHash, data := range trieData {
		args := pipeline.PipeWriteArgs{Data: data}
		err = bmtWriter.ChainWrite(&args)
		if err != nil {
			return
		}
		addr, err = boson.ParseHexAddress(expectedHash)
		if err != nil {
			return
		}
		if !bytes.Equal(args.Ref, addr.Bytes()) {
			err = ErrInvalidTrie
			return
		}
	}
	// iterate the trieData
	p := newPyramid(trieData)
	chunkAddressFunc := func(address boson.Address) error { return nil }
	isFile, e, metadata, err = s.checkIsFileWithCustomStore(ctx, p, reference)
	if err != nil {
		return
	}
	if !isFile {
		err = ErrInvalidType
		return
	}

	isManifest, m, err = s.checkIsManifestWithCustomStore(ctx, p, reference, e, metadata)
	if err != nil {
		return
	}
	if isManifest {
		// process as manifest
		err = m.IterateAddresses(ctx, func(manifestNodeAddr boson.Address) error {
			addresses, err1 := s.getDataChunksFromManifest(ctx, p, manifestNodeAddr, chunkAddressFunc)
			if err1 != nil {
				return err1
			}

			if addresses != nil {
				dataChunks = append(dataChunks, addresses)
			}
			return nil
		})
		if err != nil {
			err = fmt.Errorf("traversal: iterate chunks: %s: %w", reference, err)
			return
		}

		metadataReference := e.Metadata()

		err = s.processBytesWithCustomStore(ctx, p, metadataReference, chunkAddressFunc)

	} else {
		addresses, er := s.getDataChunksAsEntry(ctx, p, reference, chunkAddressFunc, e)
		if er != nil {
			err = er
			return
		}

		dataChunks = append(dataChunks, addresses)

	}

	// here we can put those data into localstore.
	rctx := sctx.SetRootCID(ctx, reference)
	// first we put root chunk
	_, err = s.storer.Put(rctx, storage.ModePutRequest, boson.NewChunk(reference, trieData[reference.String()]))
	if err != nil {
		return
	}
	delete(p.seen, reference.String())
	for k := range p.seen {
		addr, err = boson.ParseHexAddress(k)
		if err != nil {
			return
		}
		_, err = s.storer.Put(rctx, storage.ModePutRequest, boson.NewChunk(addr, trieData[k]))
		if err != nil {
			return
		}
	}

	return
}
