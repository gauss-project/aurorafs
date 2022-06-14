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
	"github.com/pbnjay/memory"
	"runtime"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
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
	gcBatchSize uint64 = 10000

	gcMemory uint64 = 100 * 1024 * 1024
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

			if testHookCollectGarbage != nil {
				testHookCollectGarbage(collectedCount)
			}
		case <-db.collectMemoryGarbageTrigger:
			collectedCount, done, err := db.collectMemoryGarbage()
			if err != nil {
				db.logger.Errorf("localstore: collect garbage: %v", err)
			}
			// check if another gc run is needed
			if !done {
				db.triggerGarbageCollection()
			}

			if testHookCollectGarbage != nil {
				testHookCollectGarbage(collectedCount)
			}
		case <-db.close:
			return
		}
	}
}

var dirtyGarbageNoHandle = errors.New("dirty garbage no handle")

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
	if gcSize <= target {
		return 0, true, nil
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

	defer totalTimeMetric(db.metrics.TotalTimeGCLock, time.Now())

	for _, item := range candidates {
		addr := boson.NewAddress(item.Address)
		err = db.DeleteFile(addr)
		if err != nil {
			db.metrics.GCErrorCounter.Inc()
		}
	}

	return collectedCount, done, nil
}

func (db *DB) collectMemoryGarbage() (collectedCount uint64, done bool, err error) {
	db.metrics.GCCounter.Inc()
	defer func(start time.Time) {
		if err != nil {
			db.metrics.GCErrorCounter.Inc()
		}
		totalTimeMetric(db.metrics.TotalTimeCollectGarbage, start)
	}(time.Now())
	db.batchMu.Lock()
	db.gcRunning = true
	db.batchMu.Unlock()

	defer func() {
		db.batchMu.Lock()
		db.gcRunning = false
		db.dirtyAddresses = nil
		db.batchMu.Unlock()
	}()
	done = true
	first := true
	start := time.Now()
	candidates := make([]shed.Item, 0)

	m := getMemory()

	if gcMemory < m {
		return 0, true, nil
	}
	gcMemory = gcMemory - uint64(float64(gcMemory)*gcTargetRatio)
	gcChunks := gcMemory / 256

	err = db.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if first {
			totalTimeMetric(db.metrics.TotalTimeGCFirstItem, start)
			first = false
		}

		candidates = append(candidates, item)
		collectedCount += item.GCounter
		if collectedCount >= gcChunks {
			return true, nil
		}
		return false, nil
	}, nil)
	db.metrics.GCCollectedCounter.Add(float64(collectedCount))
	if testHookGCIteratorDone != nil {
		testHookGCIteratorDone()
	}
	defer totalTimeMetric(db.metrics.TotalTimeGCLock, time.Now())

	for _, item := range candidates {
		addr := boson.NewAddress(item.Address)
		err = db.DeleteFile(addr)
		if err != nil {
			db.metrics.GCErrorCounter.Inc()
		}
	}
	return collectedCount, done, nil
}

func getMemory() uint64 {
	freemem := memory.FreeMemory()
	var memstat runtime.MemStats
	runtime.ReadMemStats(&memstat)
	freemem += (memstat.HeapInuse - memstat.HeapAlloc) + (memstat.HeapIdle - memstat.HeapReleased)
	return freemem
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

func (db *DB) triggerGarbageMemoryCollection() {
	select {
	case db.collectMemoryGarbageTrigger <- struct{}{}:
	case <-db.close:
	default:
	}
}

// incGCSizeInBatch changes gcSize field value
// by change which can be negative. This function
// must be called under batchMu lock.
func (db *DB) incGCSizeInBatch(batch driver.Batching, change int64) (err error) {
	if change == 0 {
		return nil
	}
	gcSize, err := db.gcSize.Get()
	if err != nil && !errors.Is(err, driver.ErrNotFound) {
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
	m := getMemory()
	if gcMemory > m {
		db.triggerGarbageMemoryCollection()
	}
	return nil
}

// testHookCollectGarbage is a hook that can provide
// information when a garbage collection run is done
// and how many items it removed.
var testHookCollectGarbage func(collectedCount uint64)

// testHookGCIteratorDone is a hook which is called
// when the GC is done collecting candidate items for
// eviction.
var testHookGCIteratorDone func()
