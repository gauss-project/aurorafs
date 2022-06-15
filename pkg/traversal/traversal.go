// Package traversal provides abstraction and implementation
// needed to traverse all chunks below a given root hash.
// It tries to parse all manifests and collections in its
// attempt to log all chunk addresses on the way.
package traversal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/manifest/mantaray"
)

// Traverser represents service which traverse through address dependent chunks.
type Traverser interface {
	// Traverse iterates through each address related to the supplied one, if possible.
	Traverse(context.Context, boson.Address, boson.AddressIterFunc) error
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
	ls := loadsave.NewReadonly(s.store, storage.ModeGetLookup)
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
		j, _, err := joiner.New(ctx, s.store, storage.ModeGetRequest, ref)
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

type noopChainWriter struct{}

func (n *noopChainWriter) ChainWrite(_ *pipeline.PipeWriteArgs) error { return nil }
func (n *noopChainWriter) Sum() ([]byte, error)                       { return nil, nil }

var ErrInvalidPyramid = errors.New("traversal: invalid pyramid")
