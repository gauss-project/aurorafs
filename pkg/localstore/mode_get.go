// Copyright 2018 The go-ethereum Authors
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
	"errors"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// Get returns a chunk from the database. If the chunk is
// not found storage.ErrNotFound will be returned.
// All required indexes will be updated required by the
// Getter Mode. Get is required to implement chunk.Store
// interface.
func (db *DB) Get(ctx context.Context, mode storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
	db.metrics.ModeGet.Inc()
	defer totalTimeMetric(db.metrics.TotalTimeGet, time.Now())

	defer func() {
		if err != nil {
			db.metrics.ModeGetFailure.Inc()
		}
	}()

	out, err := db.get(mode, addr, sctx.GetRootCID(ctx))
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	return boson.NewChunk(boson.NewAddress(out.Address), out.Data), nil
}

// get returns Item from the retrieval index
// and updates other indexes.
func (db *DB) get(mode storage.ModeGet, addr, rootAddr boson.Address) (out shed.Item, err error) {
	item := addressToItem(addr)

	out, err = db.retrievalDataIndex.Get(item)
	if err != nil {
		return out, err
	}
	switch mode {
	// update the access timestamp and gc index
	case storage.ModeGetRequest:
		if !rootAddr.IsZero() {
			db.updateGCItems(addressToItem(rootAddr))
		} else {
			db.updateGCItems(out)
		}

	case storage.ModeGetPin:
		pinnedItem, err := db.pinIndex.Get(item)
		if err != nil {
			return out, err
		}
		return pinnedItem, nil

	// no updates to indexes
	case storage.ModeGetSync:
	case storage.ModeGetLookup:
	default:
		return out, ErrInvalidMode
	}
	return out, nil
}

// updateGCItems is called when ModeGetRequest is used
// for Get or GetMulti to update access time and gc indexes
// for all returned chunks.
func (db *DB) updateGCItems(items ...shed.Item) {
	if db.updateGCSem != nil {
		// wait before creating new goroutines
		// if updateGCSem buffer id full
		db.updateGCSem <- struct{}{}
	}
	db.updateGCWG.Add(1)
	go func() {
		defer db.updateGCWG.Done()
		if db.updateGCSem != nil {
			// free a spot in updateGCSem buffer
			// for a new goroutine
			defer func() { <-db.updateGCSem }()
		}

		db.metrics.GCUpdate.Inc()
		defer totalTimeMetric(db.metrics.TotalTimeUpdateGC, time.Now())

		for _, item := range items {
			err := db.updateGC(item)
			if err != nil {
				db.metrics.GCUpdateError.Inc()
				db.logger.Errorf("localstore update gc: %v", err)
			}
		}
		// if gc update hook is defined, call it
		if testHookUpdateGC != nil {
			testHookUpdateGC()
		}
	}()
}

// updateGC updates garbage collection index for
// a single item. Provided item is expected to have
// only Address and Data fields with non zero values,
// which is ensured by the get function.
func (db *DB) updateGC(item shed.Item) (err error) {
	db.batchMu.Lock()
	defer db.batchMu.Unlock()
	if db.gcRunning {
		db.dirtyAddresses = append(db.dirtyAddresses, boson.NewAddress(item.Address))
	}

	batch := db.shed.NewBatch()

	// update accessTimeStamp in retrieve, gc

	i, err := db.retrievalAccessIndex.Get(item)
	switch {
	case err == nil:
		item.AccessTimestamp = i.AccessTimestamp
	case errors.Is(err, driver.ErrNotFound):
		// no chunk accesses
	default:
		return err
	}
	if item.AccessTimestamp == 0 {
		// chunk is not yet synced
		// do not add it to the gc index
		return nil
	}
	if item.BinID == 0 {
		i, err = db.retrievalDataIndex.Get(item)
		if err != nil {
			if errors.Is(err, driver.ErrNotFound) {
				return db.retrievalAccessIndex.DeleteInBatch(batch, item)
			}
			return err
		}
		item.BinID = i.BinID
	}
	// update the gc item timestamp in case
	// it exists
	var gcItem shed.Item
	gcItem, err = db.gcIndex.Get(item)
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil
		}
		return err
	}
	// delete current entry from the gc index
	err = db.gcIndex.DeleteInBatch(batch, gcItem)
	if err != nil {
		return err
	}
	item.GCounter = gcItem.GCounter
	item.AccessTimestamp = now()
	err = db.gcIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	// update retrieve access index
	err = db.retrievalAccessIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	return batch.Commit()
}

// testHookUpdateGC is a hook that can provide
// information when a garbage collection index is updated.
var testHookUpdateGC func()
