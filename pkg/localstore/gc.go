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
	"errors"
	"fmt"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	// gcTargetRatio defines the target number of items
	// in garbage collection index that will not be removed
	// on garbage collection. The target number of items
	// is calculated by gcTarget function. This value must be
	// in range (0,1]. For example, with 0.9 value,
	// garbage collection will leave 90% of defined capacity
	// in database after its run. This prevents frequent
	// garbage collection runs.
	gcTargetRatio = 0.9
	// gcBatchSize limits the number of chunks in a single
	// transaction on garbage collection.
	gcBatchSize uint64 = 2000
)

// collectGarbageWorker is a long running function that waits for
// collectGarbageTrigger channel to signal a garbage collection
// run. GC run iterates on gcIndex and removes older items
// form retrieval and other indexes.
func (db *DB) collectGarbageWorker() {
	defer close(db.collectGarbageWorkerDone)

	for {
		select {
		case <-db.collectGarbageTrigger:
			// run a single collect garbage run and
			// if done is false, gcBatchSize is reached and
			// another collect garbage run is needed
			collectedCount, done, err := db.collectGarbage()
			if err != nil {
				db.logger.Errorf("localstore: collect garbage: %v", err)
			}
			// check if another gc run is needed
			if !done {
				db.triggerGarbageCollection()
			}

			// start clean chunks
			if collectedCount > 0 {
				db.triggerGarbageRecycling()
			}

			if testHookCollectGarbage != nil {
				testHookCollectGarbage(collectedCount)
			}
		case <-db.close:
			return
		}
	}
}

// collectGarbage removes chunks from retrieval and other
// indexes if maximal number of chunks in database is reached.
// This function returns the number of removed chunks. If done
// is false, another call to this function is needed to collect
// the rest of the garbage as the batch size limit is reached.
// This function is called in collectGarbageWorker.
func (db *DB) collectGarbage() (collectedCount uint64, done bool, err error) {
	db.metrics.GCCounter.Inc()
	defer func(start time.Time) {
		if err != nil {
			db.metrics.GCErrorCounter.Inc()
		}
		totalTimeMetric(db.metrics.TotalTimeCollectGarbage, start)
	}(time.Now())
	batch := new(leveldb.Batch)
	target := db.gcTarget()

	// tell the localstore to start logging dirty addresses
	db.batchMu.Lock()
	db.gcRunning = true
	db.batchMu.Unlock()

	defer func() {
		db.batchMu.Lock()
		db.gcRunning = false
		db.dirtyAddresses = nil
		db.batchMu.Unlock()
	}()

	gcSize, err := db.gcSize.Get()
	if err != nil {
		return 0, true, err
	}
	db.metrics.GCSize.Set(float64(gcSize))

	done = true
	first := true
	start := time.Now()
	candidates := make([]shed.Item, 0)
	err = db.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if first {
			totalTimeMetric(db.metrics.TotalTimeGCFirstItem, start)
			first = false
		}
		if gcSize-collectedCount <= target {
			return true, nil
		}

		candidates = append(candidates, item)

		collectedCount += item.GCounter
		if collectedCount >= gcBatchSize {
			// batch size limit reached, however we don't
			// know whether another gc run is needed until
			// we weed out the dirty entries below
			return true, nil
		}
		return false, nil
	}, nil)
	if err != nil {
		return 0, false, err
	}
	db.metrics.GCCollectedCounter.Add(float64(collectedCount))
	if testHookGCIteratorDone != nil {
		testHookGCIteratorDone()
	}

	// protect database from changing indexes and gcSize
	db.batchMu.Lock()
	defer totalTimeMetric(db.metrics.TotalTimeGCLock, time.Now())
	defer db.batchMu.Unlock()

	// refresh gcSize value, since it might have
	// changed in the meanwhile
	gcSize, err = db.gcSize.Get()
	if err != nil {
		return 0, false, err
	}

	currentCollectedCount := uint64(0)

	// get rid of dirty entries
	for _, item := range candidates {
		addr := boson.NewAddress(item.Address)

		if addr.MemberOf(db.dirtyAddresses) {
			continue
		}

		if db.discover.IsDiscover(addr) {
			db.logger.Tracef("localstore: collect garbage: hash %s is discovering", addr)
			if gcSize-currentCollectedCount <= target {
				break
			}
			db.discover.DelDiscover(addr)
		}

		db.metrics.GCStoreTimeStamps.Set(float64(item.StoreTimestamp))
		db.metrics.GCStoreAccessTimeStamps.Set(float64(item.AccessTimestamp))
		db.metrics.GCWaitRemove.Inc()

		err = db.gcQueueIndex.PutInBatch(batch, item)
		if err != nil {
			return 0, false, err
		}
		del := db.discover.DelFile(boson.NewAddress(item.Address))
		if !del {
			return 0, false, fmt.Errorf("chunkinfo report delete %s failed", addr)
		}
		// delete from retrieve, gc
		err = db.retrievalDataIndex.DeleteInBatch(batch, item)
		if err != nil {
			return 0, false, err
		}
		err = db.retrievalAccessIndex.DeleteInBatch(batch, item)
		if err != nil {
			return 0, false, err
		}
		err = db.gcIndex.DeleteInBatch(batch, item)
		if err != nil {
			return 0, false, err
		}

		currentCollectedCount += item.GCounter
		db.logger.Tracef("localstore: collect garbage: hash %s will be clean at soon", addr)
	}
	if gcSize-currentCollectedCount > target {
		done = false
	}

	db.metrics.GCCommittedCounter.Add(float64(currentCollectedCount))
	db.gcSize.PutInBatch(batch, gcSize-currentCollectedCount)

	err = db.shed.WriteBatch(batch)
	if err != nil {
		db.metrics.GCErrorCounter.Inc()
		return 0, false, err
	}

	return currentCollectedCount, done, nil
}

// recycleGarbageWorker really remove chunks in db, triggered when
// garbage recycling is completed. This function only log a error. If
// db report serious error, we check db.close is terminated.
func (db *DB) recycleGarbageWorker() {
	var (
		recycleCount uint64
		removeChunks uint64
		closed bool
	)

	defer close(db.recycleGarbageWorkerDone)

	// if commit batch size large than gcBatchSize, done will be false.
	done := true

	for {
		batch := new(leveldb.Batch)
		candidates := make([]shed.Item, 0)

		// iterate from large to small
		err := db.gcQueueIndex.Iterate(func(item shed.Item) (stop bool, err error) {
			candidates = append(candidates, item)

			return false, nil
		}, &shed.IterateOptions{Reverse: true})
		if err != nil {
			db.logger.Errorf("localstore: recycle garbage: iterate gc queue: %v", err)
			goto next
		}

		recycleCount = 0
		removeChunks = 0

		if len(candidates) > 0 {
			for _, item := range candidates {
				addr := boson.NewAddress(item.Address)
				chunks := db.discover.GetChunkPyramid(addr)
				for _, chunk := range chunks {
					i := addressToItem(chunk.Cid)
					pin, err := db.pinIndex.Has(i)
					if err != nil {
						db.metrics.ModeHasFailure.Inc()
						db.logger.Errorf("localstore: recycle garbage: check pin failure: %v", err)
						continue
					}
					if !pin {
						exist, err := db.retrievalDataIndex.Has(i)
						if err != nil {
							db.metrics.ModeHasFailure.Inc()
							db.logger.Errorf("localstore: recycle garbage: check pin failure: %v", err)
							continue
						}
						if exist {
							err = db.retrievalDataIndex.DeleteInBatch(batch, i)
							if err != nil {
								db.logger.Errorf("localstore: recycle garbage: delete chunk data: %v", err)
								break
							}
							db.logger.Tracef("localstore: recycle garbage: chunk %s has deleted", chunk.Cid)
							removeChunks++
						}
					}

					// skip and write to db
					if removeChunks >= gcBatchSize {
						done = false
						break
					}
				}

				if done {
					err = db.gcQueueIndex.DeleteInBatch(batch, item)
					if err != nil {
						db.logger.Errorf("localstore: recycle garbage: gc queue delete: %v", err)
						db.metrics.GCErrorCounter.Inc()
						recycleCount--
					} else {
						db.logger.Tracef("localstore: recycle garbage: pyramid under hash %s has deleted", addr)
						db.discover.DelPyramid(addr)
					}
				}

				select {
				case <-db.close:
					db.logger.Warningf("localstore: recycle garbage: stops in advance due to db closing")
					closed = true
					goto writeBatch
				default:
				}
			}

		writeBatch:
			err = db.shed.WriteBatch(batch)
			if err != nil {
				db.metrics.GCErrorCounter.Inc()
				db.logger.Errorf("localstore: recycle garbage: %v", err)
			} else {
				db.metrics.GCWaitRemove.Sub(float64(recycleCount))
				db.metrics.GCRemovedCounter.Add(float64(removeChunks))
			}

			if testHookRecycleGarbage != nil {
				testHookRecycleGarbage(removeChunks)
			}

			if closed {
				return
			}
		}

	next:
		if !done {
			db.triggerGarbageRecycling()
			done = true
		}

		select {
		case <-db.recycleGarbageTrigger:
		case <-db.close:
			return
		}
	}
}

// gcTrigger retruns the absolute value for garbage collection
// target value, calculated from db.capacity and gcTargetRatio.
func (db *DB) gcTarget() (target uint64) {
	return uint64(float64(db.capacity) * gcTargetRatio)
}

// triggerGarbageCollection signals collectGarbageWorker
// to call collectGarbage.
func (db *DB) triggerGarbageCollection() {
	select {
	case db.collectGarbageTrigger <- struct{}{}:
	case <-db.close:
	default:
	}
}

// triggerGarbageRecycling signals recycleGarbageWorker
// to call recycleGarbage
func (db *DB) triggerGarbageRecycling() {
	select {
	case db.recycleGarbageTrigger <- struct{}{}:
	case <-db.close:
	default:
	}
}

// incGCSizeInBatch changes gcSize field value
// by change which can be negative. This function
// must be called under batchMu lock.
func (db *DB) incGCSizeInBatch(batch *leveldb.Batch, change int64) (err error) {
	if change == 0 {
		return nil
	}
	gcSize, err := db.gcSize.Get()
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return err
	}

	var newSize uint64
	if change > 0 {
		newSize = gcSize + uint64(change)
	} else {
		// 'change' is an int64 and is negative
		// a conversion is needed with correct sign
		c := uint64(-change)
		if c > gcSize {
			// protect uint64 undeflow
			return nil
		}
		newSize = gcSize - c
	}
	db.gcSize.PutInBatch(batch, newSize)
	db.metrics.GCSize.Set(float64(newSize))

	// trigger garbage collection if we reached the capacity
	if newSize >= db.capacity {
		db.triggerGarbageCollection()
	}
	return nil
}

// testHookCollectGarbage is a hook that can provide
// information when a garbage collection run is done
// and how many items it removed.
var testHookCollectGarbage func(collectedCount uint64)

// testHookCollectGarbage is a hook that can provide
// information when a garbage recycling run is done
// and how many items it removed.
var testHookRecycleGarbage func(recycledCount uint64)

// testHookGCIteratorDone is a hook which is called
// when the GC is done collecting candidate items for
// eviction.
var testHookGCIteratorDone func()
