// Package traversal provides abstraction and implementation
// needed to traverse all chunks below a given root hash.
// It tries to parse all manifests and collections in its
// attempt to log all chunk addresses on the way.
package traversal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/bmt"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/manifest/mantaray"
)

// Traverser represents service which traverse through address dependent chunks.
type Traverser interface {
	// Traverse iterates through each address related to the supplied one, if possible.
	Traverse(context.Context, boson.Address, boson.AddressIterFunc) error
	// GetPyramid returns pyramid hash and its contains data.
	GetPyramid(context.Context, boson.Address) (map[string][]byte, error)
	// GetChunkHashes returns all stored data chunk related to the given reference, if possible.
	GetChunkHashes(context.Context, boson.Address, map[string][]byte) ([][][]byte, [][]byte, error)
}

type PutGetter interface {
	storage.Putter
	storage.Getter
}

// New constructs for a new Traverser.
func New(store PutGetter) Traverser {
	return &service{store: store}
}

// service is implementation of Traverser using storage.Storer as its storage.
type service struct {
	store PutGetter
}

// traverseAndProcess traverses the given reference on a storage and applies processFn on each chunk.
func (s *service) traverseAndProcess(ctx context.Context, addr boson.Address, processFn boson.AddressIterFunc) error {
	ls := loadsave.NewReadonly(s.store)
	switch mf, err := manifest.NewDefaultManifestReference(addr, ls); {
	case errors.Is(err, manifest.ErrInvalidManifestType):
		break
	case err != nil:
		return fmt.Errorf("traversal: unable to create manifest reference for %q: %w", addr, err)
	default:
		err := mf.IterateAddresses(ctx, processFn)
		if errors.Is(err, mantaray.ErrTooShort) || errors.Is(err, mantaray.ErrInvalidVersionHash) {
			// Based on the returned errors we conclude that it might
			// not be a manifest, so we try non-manifest processing.
			break
		}
		if err != nil {
			return fmt.Errorf("traversal: unable to process bytes for %q: %w", addr, err)
		}
		return nil
	}

	// Non-manifest processing.
	if err := processFn(addr); err != nil {
		return fmt.Errorf("traversal: unable to process bytes for %q: %w", addr, err)
	}
	return nil
}

// Traverse implements Traverser.Traverse method.
func (s *service) Traverse(ctx context.Context, addr boson.Address, iterFn boson.AddressIterFunc) error {
	processBytes := func(ref boson.Address) error {
		j, _, err := joiner.New(ctx, s.store, ref)
		if err != nil {
			return fmt.Errorf("traversal: joiner error on %q: %w", ref, err)
		}
		err = j.IterateChunkAddresses(iterFn)
		if err != nil {
			return fmt.Errorf("traversal: iterate chunk address error for %q: %w", ref, err)
		}
		return nil
	}

	return s.traverseAndProcess(ctx, addr, processBytes)
}

// dataWithSpan returns chunk filled with span length
func dataWithSpan(data []byte, size uint64) []byte {
	spanData := make([]byte, len(data)+8)
	if size == 0 {
		size = uint64(len(data))
	}
	binary.LittleEndian.PutUint64(spanData[:8], size)
	copy(spanData[8:], data)
	return spanData
}

// GetPyramid implements Traverser.GetPyramid method.
func (s *service) GetPyramid(ctx context.Context, addr boson.Address) (pyramid map[string][]byte, err error) {
	storePyramidHashes := func(ref boson.Address) error {
		j, span, err := joiner.New(ctx, s.store, ref)
		if err != nil {
			return fmt.Errorf("traversal: joiner error on %q: %w", ref, err)
		}
		// for one chunk, it should save file chunk for known file size.
		pyramid[ref.String()] = dataWithSpan(j.GetRootData(), uint64(span))
		if span > boson.ChunkSize {
			j.SetSaveEdgeChunks(pyramid)
			if err := j.IterateChunkAddresses(func(addr boson.Address) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}

	pyramid = make(map[string][]byte)
	err = s.traverseAndProcess(ctx, addr, storePyramidHashes)

	return
}

type noopChainWriter struct{}

func (n *noopChainWriter) ChainWrite(_ *pipeline.PipeWriteArgs) error { return nil }
func (n *noopChainWriter) Sum() ([]byte, error)                       { return nil, nil }

var ErrInvalidPyramid = errors.New("traversal: invalid pyramid")

// GetChunkHashes implements Traverser.GetChunkHashes method.
func (s *service) GetChunkHashes(ctx context.Context, addr boson.Address, pyramid map[string][]byte) (hashes [][][]byte, pieces [][]byte, err error) {
	var (
		store     PutGetter
		uploading bool
	)

	if pyramid == nil {
		uploading = true
	}

	// define how to store DATA chunk
	storeChunkHashes := func(nodeType int, path, prefix, hash []byte, metadata map[string]string) error {
		switch nodeType {
		case int(manifest.File):
			ref := boson.NewAddress(hash)
			j, span, err := joiner.New(ctx, store, ref)
			if err != nil {
				return fmt.Errorf("traversal: joiner error on %q: %w", ref, err)
			}
			if !uploading && span <= boson.ChunkSize {
				pieces = append(pieces, hash)
			}
			j.SetSaveDataChunks()
			if err := j.IterateChunkAddresses(func(addr boson.Address) error { return nil }); err != nil {
				return err
			}
			hashes = append(hashes, j.GetDataChunks())
		}
		return nil
	}

	pieces = make([][]byte, 0)

	if !uploading {
		if _, exists := pyramid[addr.String()]; !exists {
			return nil, nil, fmt.Errorf("invalid pyramid without reference %s\n", addr)
		}

		// verify each data could be sum up the correct hash.
		bmtWriter := bmt.NewBmtWriter(&noopChainWriter{})
		for hash, data := range pyramid {
			var ref boson.Address
			args := pipeline.PipeWriteArgs{Data: data}
			err = bmtWriter.ChainWrite(&args)
			if err != nil {
				return
			}
			ref, err = boson.ParseHexAddress(hash)
			if err != nil {
				return
			}
			if !bytes.Equal(args.Ref, ref.Bytes()) {
				err = ErrInvalidPyramid
				return
			}
		}

		// iterate the given pyramid
		p := newPyramid(pyramid)
		store = p

		defer func() {
			if err != nil {
				return
			}
			// here we can put those data into localstore.
			rctx := sctx.SetRootCID(ctx, addr)
			// first we put root chunk
			_, err = s.store.Put(rctx, storage.ModePutRequest, boson.NewChunk(addr, pyramid[addr.String()]))
			if err != nil {
				return
			}
			delete(p.seen, addr.String())
			for k := range p.seen {
				addr, err = boson.ParseHexAddress(k)
				if err != nil {
					return
				}
				_, err = s.store.Put(rctx, storage.ModePutRequest, boson.NewChunk(addr, pyramid[k]))
				if err != nil {
					return
				}
			}
		}()
	} else {
		store = s.store
	}

	// process as manifest
	processNodes := func() error {
		ls := loadsave.NewReadonly(store)
		switch mf, err := manifest.NewDefaultManifestReference(addr, ls); {
		case errors.Is(err, manifest.ErrInvalidManifestType):
			break
		case err != nil:
			return fmt.Errorf("traversal: unable to create manifest reference for %q: %w", addr, err)
		default:
			// iterate as hierarchical directory
			err := mf.IterateDirectories(ctx, []byte{}, -1, storeChunkHashes)
			if errors.Is(err, mantaray.ErrTooShort) || errors.Is(err, mantaray.ErrInvalidVersionHash) {
				break
			}
			if err != nil {
				return fmt.Errorf("traversal: unable to process bytes for %q: %w", addr, err)
			}
			return nil
		}

		// fallback to OLD FILE iteration
		if err := storeChunkHashes(int(manifest.File), []byte{}, []byte{}, addr.Bytes(), nil); err != nil {
			return fmt.Errorf("traversal: unable to process bytes for %q: %w", addr, err)
		}

		return nil
	}

	// process as manifest
	err = processNodes()

	return
}

type pyramid struct {
	data map[string][]byte
	seen map[string]struct{}
	mu   sync.Mutex
}

func newPyramid(data map[string][]byte) *pyramid {
	return &pyramid{
		data: data,
		seen: make(map[string]struct{}),
	}
}

func (p *pyramid) Get(ctx context.Context, _ storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
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

func (p *pyramid) Has(_ context.Context, _ storage.ModeHas, _ boson.Address) (bool, error) {
	panic("not implemented")
}

func (p *pyramid) HasMulti(_ context.Context, _ storage.ModeHas, _ ...boson.Address) ([]bool, error) {
	panic("not implemented")
}

func (p *pyramid) Close() error {
	return nil
}
