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
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

type store struct {
	storage.Storer
	retrieval retrieval.Interface
	logger    logging.Logger
	//recoveryCallback recovery.Callback // this is the callback to be executed when a chunk fails to be retrieved
}

var (
	ErrRecoveryAttempt = errors.New("failed to retrieve chunk, recovery initiated")
)

// New returns a new NetStore that wraps a given Storer.
func New(s storage.Storer, r retrieval.Interface, logger logging.Logger) storage.Storer {
	return &store{Storer: s, retrieval: r, logger: logger}
}

// Get retrieves a given chunk address.
// It will request a chunk from the network whenever it cannot be found locally.
func (s *store) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
	ch, err = s.Storer.Get(ctx, mode, addr)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			rootCID := sctx.GetRootCID(ctx)
			if !rootCID.IsZero() {
				// request from network
				ch, err = s.retrieval.RetrieveChunk(ctx, rootCID, addr)
				if err != nil {
					return nil, storage.ErrNotFound
				}

				return ch, nil
			}
			return nil, err
		}

		return nil, fmt.Errorf("netstore get: %w", err)
	}
	return ch, nil
}
