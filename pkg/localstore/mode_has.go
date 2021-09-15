// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package localstore

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

// Has returns true if the chunk is stored in database.
func (db *DB) Has(ctx context.Context, mode storage.ModeHas, addr boson.Address) (bool, error) {
	var has bool
	var err error

	db.metrics.ModeHas.Inc()
	defer totalTimeMetric(db.metrics.TotalTimeHas, time.Now())
	switch mode {
	case storage.ModeHasPin:
		has, err = db.pinIndex.Has(addressToItem(addr))
	case storage.ModeHasChunk:
		has, err = db.retrievalDataIndex.Has(addressToItem(addr))
	default:
		return false, ErrInvalidMode
	}

	if err != nil {
		db.metrics.ModeHasFailure.Inc()
	}
	return has, err
}

// HasMulti returns a slice of booleans which represent if the provided chunks
// are stored in database.
func (db *DB) HasMulti(ctx context.Context, hasMode storage.ModeHas, addrs ...boson.Address) ([]bool, error) {

	var have []bool
	var err error

	db.metrics.ModeHasMulti.Inc()

	switch hasMode {
	case storage.ModeHasPin:
		have, err = db.pinIndex.HasMulti(addressesToItems(addrs...)...)
	case storage.ModeHasChunk:
		have, err = db.retrievalDataIndex.HasMulti(addressesToItems(addrs...)...)
	default:
		have, err = db.retrievalDataIndex.HasMulti(addressesToItems(addrs...)...)
	}
	defer totalTimeMetric(db.metrics.TotalTimeHasMulti, time.Now())

	if err != nil {
		db.metrics.ModeHasMultiFailure.Inc()
	}
	return have, err
}
