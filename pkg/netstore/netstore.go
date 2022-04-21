// Package netstore provides an abstraction layer over the
// Aurora local storage layer that leverages connectivity
// with other peers in order to retrieve chunks from the network that cannot
// be found locally.
package netstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

type Store struct {
	storage.Storer
	retrieval retrieval.Interface
	logger    logging.Logger
	chunkInfo chunkinfo.Interface
	addr      boson.Address
	// recoveryCallback recovery.Callback // this is the callback to be executed when a chunk fails to be retrieved
}

var (
	ErrRecoveryAttempt = errors.New("failed to retrieve chunk, recovery initiated")
)

// New returns a new NetStore that wraps a given Storer.
func New(s storage.Storer, r retrieval.Interface, logger logging.Logger, addr boson.Address) *Store {
	return &Store{Storer: s, retrieval: r, logger: logger, addr: addr}
}

func (s *Store) SetChunkInfo(chunkInfo chunkinfo.Interface) {
	s.chunkInfo = chunkInfo
}

// Get retrieves a given chunk address.
// It will request a chunk from the network whenever it cannot be found locally.
func (s *Store) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
	rootHash := sctx.GetRootHash(ctx)
	ch, err = s.Storer.Get(ctx, mode, addr)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if rootHash.IsZero() {
				return nil, err
			}
			// request from network
			ch, err = s.retrieval.RetrieveChunk(ctx, rootHash, addr)
			if err != nil {
				return nil, ErrRecoveryAttempt
			}
			return ch, nil
		}

		return nil, fmt.Errorf("netstore get: %w", err)
	}
	if !rootHash.IsZero() && !rootHash.Equal(addr) {
		_ = s.chunkInfo.OnChunkRetrieved(addr, rootHash, s.addr)
	}

	return ch, nil
}
