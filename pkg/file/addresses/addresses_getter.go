package addresses

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

type addressesGetterStore struct {
	getter storage.Getter
	fn     boson.AddressIterFunc
}

// NewGetter creates a new proxy storage.Getter which calls provided function
// for each chunk address processed.
func NewGetter(getter storage.Getter, fn boson.AddressIterFunc) storage.Getter {
	return &addressesGetterStore{getter, fn}
}

func (s *addressesGetterStore) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address, index int64) (boson.Chunk, error) {
	ch, err := s.getter.Get(ctx, mode, addr, index)
	if err != nil {
		return nil, err
	}

	return ch, s.fn(ch.Address())
}
