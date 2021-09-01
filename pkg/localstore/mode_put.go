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
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/syndtr/goleveldb/leveldb"
)

type RootCIDKey struct{}

// Put stores Chunks to database and depending
// on the Putter mode, it updates required indexes.
// Put is required to implement storage.Store
// interface.
func (db *DB) Put(ctx context.Context, mode storage.ModePut, chs ...boson.Chunk) (exist []bool, err error) {
	rootCID, ok := ctx.Value(RootCIDKey{}).(boson.Address)
	if !ok {
		rootCID = boson.ZeroAddress
	}

	db.metrics.ModePut.Inc()
	defer totalTimeMetric(db.metrics.TotalTimePut, time.Now())

	exist, err = db.put(mode, rootCID, chs...)
	if err != nil {
		db.metrics.ModePutFailure.Inc()
	}

	return exist, err
}

// put stores Chunks to database and updates other indexes. It acquires lockAddr
// to protect two calls of this function for the same address in parallel. Item
// fields Address and Data must not be with their nil values. If chunks with the
// same address are passed in arguments, only the first chunk will be stored,
// and following ones will have exist set to true for their index in exist
// slice. This is the same behaviour as if the same chunks are passed one by one
// in multiple put method calls.
func (db *DB) put(mode storage.ModePut, rootCID boson.Address, chs ...boson.Chunk) (exist []bool, err error) {
	// this is an optimization that tries to optimize on already existing chunks
	// not needing to acquire batchMu. This is in order to reduce lock contention
	// when chunks are retried across the network for whatever reason.
	if len(chs) == 1 && mode != storage.ModePutRequestPin && mode != storage.ModePutUploadPin {
		has, err := db.retrievalDataIndex.Has(chunkToItem(chs[0]))
		if err != nil {
			return nil, err
		}
		if has {
			return []bool{true}, nil
		}
	}

	// protect parallel updates
	db.batchMu.Lock()
	defer db.batchMu.Unlock()
	if db.gcRunning {
		for _, ch := range chs {
			db.dirtyAddresses = append(db.dirtyAddresses, ch.Address())
		}
	}

	batch := new(leveldb.Batch)

	// variables that provide information for operations
	// to be done after write batch function successfully executes
	var gcSizeChange int64                      // number to add or subtract from gcSize

	exist = make([]bool, len(chs))

	// A lazy populated map of bin ids to properly set
	// BinID values for new chunks based on initial value from database
	// and incrementing them.
	// Values from this map are stored with the batch
	binIDs := make(map[uint8]uint64)

	switch mode {
	case storage.ModePutRequest, storage.ModePutRequestPin:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				exist[i] = true
				continue
			}
			exists, err := db.putRequest(batch, binIDs, chunkToItem(ch))
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			gcSizeChange++

			if mode == storage.ModePutRequestPin {
				err = db.setPin(batch, ch.Address())
				if err != nil {
					return nil, err
				}
			}
		}

	case storage.ModePutUpload, storage.ModePutUploadPin:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				exist[i] = true
				continue
			}
			exists, err := db.putUpload(batch, binIDs, chunkToItem(ch))
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			if mode == storage.ModePutUploadPin {
				err = db.setPin(batch, ch.Address())
				if err != nil {
					return nil, err
				}
			}
		}

	default:
		return nil, ErrInvalidMode
	}

	for po, id := range binIDs {
		db.binIDs.PutInBatch(batch, uint64(po), id)
	}

	if !rootCID.IsZero() {
		item := addressToItem(rootCID)
		i, err := db.retrievalAccessIndex.Get(item)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		} else {
			item.AccessTimestamp = i.AccessTimestamp
			i, err = db.retrievalDataIndex.Get(item)
			if err != nil {
				return nil, err
			}
			item.BinID = i.BinID
			gcItem, err := db.gcIndex.Get(item)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					gcItem.BinID = item.BinID
					gcItem.Address = item.Address
					gcItem.AccessTimestamp = item.AccessTimestamp
					gcItem.GCounter = uint64(gcSizeChange)
				} else {
					return nil, err
				}
			} else {
				err = db.gcIndex.DeleteInBatch(batch, item)
				if gcSizeChange >= 0 {
					gcItem.GCounter += uint64(gcSizeChange)
				} else {
					c := uint64(-gcSizeChange)
					if c > gcItem.GCounter {
						gcItem.GCounter = 0
					} else {
						gcItem.GCounter -= c
					}
				}
			}
			db.gcIndex.PutInBatch(batch, gcItem)
		}
	}

	err = db.incGCSizeInBatch(batch, gcSizeChange)
	if err != nil {
		return nil, err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return nil, err
	}

	return exist, nil
}

// putRequest adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, gc
//  - it does not enter the syncpool
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putRequest(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, err error) {
	has, err := db.retrievalDataIndex.Has(item)
	if err != nil {
		return false,  err
	}
	if has {
		return true,  nil
	}

	item.StoreTimestamp = now()
	item.BinID, err = db.incBinID(binIDs, db.po(boson.NewAddress(item.Address)))
	if err != nil {
		return false,  err
	}

	err = db.retrievalDataIndex.PutInBatch(batch, item)
	if err != nil {
		return false,  err
	}

	item.AccessTimestamp = now()
	err = db.retrievalAccessIndex.PutInBatch(batch, item)
	if err != nil {
		return false,  err
	}

	return false, nil
}

// putUpload adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, push, pull
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putUpload(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, err error) {
	exists, err = db.retrievalDataIndex.Has(item)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	item.StoreTimestamp = now()
	item.BinID, err = db.incBinID(binIDs, db.po(boson.NewAddress(item.Address)))
	if err != nil {
		return false, err
	}
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	if err != nil {
		return false, err
	}

	return false, nil
}

// putSync adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, pull, gc
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putSync(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, gcSizeChange int64, err error) {
	exists, err = db.retrievalDataIndex.Has(item)
	if err != nil {
		return false, 0, err
	}
	if exists {
		return true, 0, nil
	}

	item.StoreTimestamp = now()
	item.BinID, err = db.incBinID(binIDs, db.po(boson.NewAddress(item.Address)))
	if err != nil {
		return false, 0, err
	}
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	if err != nil {
		return false, 0, err
	}
	gcSizeChange, err = db.setGC(batch, item)
	if err != nil {
		return false, 0, err
	}

	return false, gcSizeChange, nil
}

// setGC is a helper function used to add chunks to the retrieval access
// index and the gc index in the cases that the putToGCCheck condition
// warrants a gc set. this is to mitigate index leakage in edge cases where
// a chunk is added to a node's localstore and given that the chunk is
// already within that node's NN (thus, it can be added to the gc index
// safely)
func (db *DB) setGC(batch *leveldb.Batch, item shed.Item) (gcSizeChange int64, err error) {
	if item.BinID == 0 {
		i, err := db.retrievalDataIndex.Get(item)
		if err != nil {
			return 0, err
		}
		item.BinID = i.BinID
	}
	i, err := db.retrievalAccessIndex.Get(item)
	switch {
	case err == nil:
		item.AccessTimestamp = i.AccessTimestamp
		err = db.gcIndex.DeleteInBatch(batch, item)
		if err != nil {
			return 0, err
		}
		gcSizeChange--
	case errors.Is(err, leveldb.ErrNotFound):
		// the chunk is not accessed before
	default:
		return 0, err
	}
	item.AccessTimestamp = now()
	err = db.retrievalAccessIndex.PutInBatch(batch, item)
	if err != nil {
		return 0, err
	}

	// add new entry to gc index ONLY if it is not present in pinIndex
	ok, err := db.pinIndex.Has(item)
	if err != nil {
		return 0, err
	}
	if !ok {
		err = db.gcIndex.PutInBatch(batch, item)
		if err != nil {
			return 0, err
		}
		gcSizeChange++
	}

	return gcSizeChange, nil
}

// incBinID is a helper function for db.put* methods that increments bin id
// based on the current value in the database. This function must be called under
// a db.batchMu lock. Provided binID map is updated.
func (db *DB) incBinID(binIDs map[uint8]uint64, po uint8) (id uint64, err error) {
	if _, ok := binIDs[po]; !ok {
		binIDs[po], err = db.binIDs.Get(uint64(po))
		if err != nil {
			return 0, err
		}
	}
	binIDs[po]++
	return binIDs[po], nil
}

// containsChunk returns true if the chunk with a specific address
// is present in the provided chunk slice.
func containsChunk(addr boson.Address, chs ...boson.Chunk) bool {
	for _, c := range chs {
		if addr.Equal(c.Address()) {
			return true
		}
	}
	return false
}
