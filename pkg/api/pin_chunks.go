package api

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

func (s *server) pinChunkAddressFn(ctx context.Context, reference boson.Address) func(address boson.Address) error {
	return func(address boson.Address) error {
		// NOTE: stop pinning on first error

		err := s.storer.Set(ctx, storage.ModeSetPin, address)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// chunk not found locally, try to get from netstore
				ch, err := s.storer.Get(ctx, storage.ModeGetRequest, address)
				if err != nil {
					s.logger.Debugf("pin traversal: storer get: for reference %s, address %s: %w", reference, address, err)
					return err
				}

				_, err = s.storer.Put(ctx, storage.ModePutRequestPin, ch)
				if err != nil {
					s.logger.Debugf("pin traversal: storer put pin: for reference %s, address %s: %w", reference, address, err)
					return err
				}

				return nil
			}

			s.logger.Debugf("pin traversal: storer set pin: for reference %s, address %s: %w", reference, address, err)
			return err
		}

		return nil
	}
}

func (s *server) unpinChunkAddressFn(ctx context.Context, reference boson.Address) func(address boson.Address) error {
	return func(address boson.Address) error {

		err := s.storer.Set(ctx, storage.ModeSetUnpin, address)
		if err != nil {
			s.logger.Debugf("unpin traversal: for reference %s, address %s: %w", reference, address, err)
			// continue un-pinning all chunks
		}

		return nil
	}
}
