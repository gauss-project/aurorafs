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
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	chunkinfo "github.com/gauss-project/aurorafs/pkg/chunkinfo/mock"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

// TestDB_collectGarbageWorker tests garbage collection runs
// by uploading and syncing a number of chunks.
func TestDB_collectGarbageWorker(t *testing.T) {
	testDBCollectGarbageWorker(t)
}

// TestDB_collectGarbageWorker_multipleBatches tests garbage
// collection runs by uploading and syncing a number of
// chunks by having multiple smaller batches.
func TestDB_collectGarbageWorker_multipleBatches(t *testing.T) {
	// lower the maximal number of chunks in a single
	// gc batch to ensure multiple batches.
	defer func(s uint64) { gcBatchSize = s }(gcBatchSize)
	gcBatchSize = 2

	testDBCollectGarbageWorker(t)
}

// testDBCollectGarbageWorker is a helper test function to test
// garbage collection runs by put request a number of chunks.
func testDBCollectGarbageWorker(t *testing.T) {

	chunkCount := 150

	var closed chan struct{}
	testHookCollectGarbageChan := make(chan uint64)
	testHookRecycleGarbageChan := make(chan uint64)
	t.Cleanup(setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		select {
		case testHookCollectGarbageChan <- collectedCount:
		case <-closed:
		}
	}))
	t.Cleanup(setTestHook(&testHookRecycleGarbage, func(recycledCount uint64) {
		select {
		case testHookRecycleGarbageChan <- recycledCount:
		case <-closed:
		}
	}))
	db := newTestDB(t, &Options{
		Capacity: 100,
	})
	closed = db.close

	// upload random file
	reference, chunks, addGc := addRandomFile(t, chunkCount, db, ci, false)

	addGc(t)

	// trigger gc
	db.triggerGarbageCollection()
	gcTarget := db.gcTarget()

	for {
		select {
		case <-testHookCollectGarbageChan:
		case <-time.After(10 * time.Second):
			t.Error("collect garbage timeout")
		}
		gcSize, err := db.gcSize.Get()
		if err != nil {
			t.Fatal(err)
		}
		if gcSize <= gcTarget {
			break
		}
	}

	t.Run("gc index count", newItemsCountTest(db.gcIndex, 0))

	t.Run("gc size", newIndexGCSizeTest(db))

	// the file reference chunk should be removed
	t.Run("get the file reference chunk", func(t *testing.T) {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, reference)
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got error %v, want %v", err, storage.ErrNotFound)
		}
	})

	var prevChunkSize int

	for {
		select {
		case <-testHookRecycleGarbageChan:
		case <-time.After(10 * time.Second):
			t.Error("recycle garbage timeout")
		}
		chunkSize, err := db.retrievalDataIndex.Count()
		if err != nil {
			t.Fatal(err)
		}
		prevChunkSize = chunkSize
		if chunkSize == 0 {
			break
		}
		if prevChunkSize > 0 && prevChunkSize < chunkSize {
			t.Errorf("got current chunk size %d, but prev chunk size (%d) small than this\n", chunkSize, prevChunkSize)
		}
	}

	t.Run("file related chunks should be removed", func(t *testing.T) {
		for i := 0; i < chunkCount; i++ {
			_, err := db.Get(context.Background(), storage.ModeGetRequest, chunks[i])
			if !errors.Is(err, storage.ErrNotFound) {
				t.Errorf("got error %v, want %v", err, storage.ErrNotFound)
			}
		}
	})
}

// Pin a file, upload chunks to go past the gc limit to trigger GC,
// check if the pinned files are still around and removed from gcIndex
func TestPinGC(t *testing.T) {

	chunkCount := 150
	cacheCapacity := uint64(100)

	var closed chan struct{}
	testHookCollectGarbageChan := make(chan uint64)
	testHookRecycleGarbageChan := make(chan uint64)
	t.Cleanup(setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		select {
		case testHookCollectGarbageChan <- collectedCount:
		case <-closed:
		}
	}))
	t.Cleanup(setTestHook(&testHookRecycleGarbage, func(collectedCount uint64) {
		select {
		case testHookRecycleGarbageChan <- collectedCount:
		case <-closed:
		}
	}))

	db := newTestDB(t, &Options{
		Capacity: cacheCapacity,
	})
	closed = db.close

	// upload random file
	_, chunksA, addGC1 := addRandomFile(t, int(cacheCapacity)-5, db, ci, false)
	addGC1(t)
	pinReference, pinChunks, _ := addRandomFile(t, chunkCount, db, ci, true)
	_, chunksB, addGC2 := addRandomFile(t, 5, db, ci, false)
	addGC2(t)

	curSize, err := db.gcSize.Get()
	if err != nil {
		t.Fatal(err)
	}
	if curSize < db.capacity {
		t.Fatalf("current db not trigger gc， cur gc size = %d\n", curSize)
	}

	db.triggerGarbageCollection()
	gcTarget := db.gcTarget()

	for {
		select {
		case <-testHookCollectGarbageChan:
		case <-time.After(10 * time.Second):
			t.Error("collect garbage timeout")
		}
		gcSize, err := db.gcSize.Get()
		if err != nil {
			t.Fatal(err)
		}
		if gcSize <= gcTarget {
			break
		}
	}

	t.Run("pin Index count", newItemsCountTest(db.pinIndex, len(pinChunks)))

	t.Run("gc index count", newItemsCountTest(db.gcIndex, 1))

	t.Run("gc size", func(t *testing.T) {
		got, err := db.gcSize.Get()
		if err != nil {
			t.Fatal(err)
		}
		if got != uint64(len(chunksB)) {
			t.Errorf("got gc size %v, want %v", got, len(chunksB))
		}
	})

	select {
	case <-testHookRecycleGarbageChan:
	case <-time.After(10 * time.Second):
		t.Error("recycle garbage timeout")
	}

	t.Run("first upload chunk was removed from gc index", func(t *testing.T) {
		for _, hash := range chunksA {
			_, err := db.Get(context.Background(), storage.ModeGetRequest, hash)
			if !errors.Is(err, storage.ErrNotFound) {
				t.Fatal(err)
			}
		}
	})

	t.Run("pinned chunk not in gc Index", func(t *testing.T) {
		err := db.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
			if bytes.Equal(pinReference.Bytes(), item.Address) {
				t.Fatal("pin chunk present in gcIndex")
			}
			return false, nil
		}, nil)
		if err != nil {
			t.Fatal("could not iterate gcIndex")
		}
	})

	t.Run("pinned chunks exists", func(t *testing.T) {
		for _, hash := range pinChunks {
			_, err := db.Get(context.Background(), storage.ModeGetRequest, hash)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("second upload chunks exists", func(t *testing.T) {
		for _, hash := range chunksB {
			_, err := db.Get(context.Background(), storage.ModeGetRequest, hash)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

// Upload chunks, pin those chunks, add to GC after it is pinned
// check if the pinned files are still around
func TestGCAfterPin(t *testing.T) {

	chunkCount := 50

	db := newTestDB(t, &Options{
		Capacity: 100,
	})

	// upload random chunks
	reference, chunks, addGc := addRandomFile(t, chunkCount, db, ci, false)

	ctx := sctx.SetRootCID(context.Background(), reference)
	for _, chunk := range chunks {
		err := db.Set(ctx, storage.ModeSetPin, chunk)
		if err != nil {
			t.Fatal(err)
		}
	}

	addGc(t)

	t.Run("pin Index count", newItemsCountTest(db.pinIndex, len(chunks)))

	t.Run("gc index count", newItemsCountTest(db.gcIndex, 0))

	for _, hash := range chunks {
		_, err := db.Get(ctx, storage.ModeGetRequest, hash)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestDB_collectGarbageWorker_withRequests is a helper test function
// to test garbage collection runs by uploading, syncing and
// requesting a number of chunks.
func TestDB_collectGarbageWorker_withRequests(t *testing.T) {
	db := newTestDB(t, &Options{
		Capacity: 100,
	})

	testHookCollectGarbageChan := make(chan uint64)
	defer setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		testHookCollectGarbageChan <- collectedCount
	})()

	addrs := make([]boson.Address, 0)

	// upload random chunks just up to the capacity
	for i := 0; i < int(db.capacity)-1; i++ {
		ch := generateTestRandomChunk()

		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}

		addrs = append(addrs, ch.Address())
	}

	// set update gc test hook to signal when
	// update gc goroutine is done by closing
	// testHookUpdateGCChan channel
	testHookUpdateGCChan := make(chan struct{})
	resetTestHookUpdateGC := setTestHookUpdateGC(func() {
		close(testHookUpdateGCChan)
	})

	// request the latest put chunk
	// to prioritize it in the gc index
	// not to be collected
	ctx := sctx.SetRootCID(context.Background(), addrs[0])
	_, err := db.Get(ctx, storage.ModeGetRequest, addrs[0])
	if err != nil {
		t.Fatal(err)
	}

	// wait for update gc goroutine to finish for garbage
	// collector to be correctly triggered after the last upload
	select {
	case <-testHookUpdateGCChan:
	case <-time.After(10 * time.Second):
		t.Fatal("updateGC was not called after getting chunk with ModeGetRequest")
	}

	// no need to wait for update gc hook anymore
	resetTestHookUpdateGC()

	// put request another chunk to trigger
	// garbage collection
	ch := generateTestRandomChunk()
	ctx = sctx.SetRootCID(context.Background(), ch.Address())
	_, err = db.Put(ctx, storage.ModePutRequest, ch)
	if err != nil {
		t.Fatal(err)
	}
	addrs = append(addrs, ch.Address())

	// wait for garbage collection
	gcTarget := db.gcTarget()

	var totalCollectedCount uint64
	for {
		select {
		case c := <-testHookCollectGarbageChan:
			totalCollectedCount += c
		case <-time.After(10 * time.Second):
			t.Error("collect garbage timeout")
		}
		gcSize, err := db.gcSize.Get()
		if err != nil {
			t.Fatal(err)
		}
		if gcSize == gcTarget {
			break
		}
	}

	wantTotalCollectedCount := uint64(len(addrs)) - gcTarget
	if totalCollectedCount != wantTotalCollectedCount {
		t.Errorf("total collected chunks %v, want %v", totalCollectedCount, wantTotalCollectedCount)
	}

	t.Run("gc index count", newItemsCountTest(db.gcIndex, int(gcTarget)))

	t.Run("gc size", newIndexGCSizeTest(db))

	// requested chunk should not be removed
	t.Run("get requested chunk", func(t *testing.T) {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, addrs[0])
		if err != nil {
			t.Fatal(err)
		}
	})

	// the second synced chunk should be removed
	t.Run("get gc-ed chunk", func(t *testing.T) {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, addrs[1])
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got error %v, want %v", err, storage.ErrNotFound)
		}
	})

	// last synced chunk should not be removed
	t.Run("get most recent synced chunk", func(t *testing.T) {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, addrs[len(addrs)-1])
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestDB_gcSize checks if gcSize has a correct value after
// database is initialized with existing data.
func TestDB_gcSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "localstore-stored-gc-size")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	baseKey := make([]byte, 32)
	if _, err := rand.Read(baseKey); err != nil {
		t.Fatal(err)
	}
	logger := logging.New(io.Discard, 0)
	db, err := New(dir, baseKey, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	db.Config(ci)
	count := 100

	for i := 0; i < count; i++ {
		ch := generateTestRandomChunk()

		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New(dir, baseKey, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	db.Config(ci)
	defer db.Close()

	t.Run("gc index size", newIndexGCSizeTest(db))
}

// setTestHook sets gc hook and
// returns a function that will reset it to the
// value before the change.
func setTestHook(hook *func(uint64), f func(collectedCount uint64)) (reset func()) {
	current := *hook
	reset = func() { testHookCollectGarbage = current }
	*hook = f
	return reset
}

// TestSetTestHookCollectGarbage tests if setTestHookCollectGarbage changes
// testHookCollectGarbage function correctly and if its reset function
// resets the original function.
func TestSetTestHook(t *testing.T) {
	// Set the current function after the test finishes.
	defer func(h func(collectedCount uint64)) { testHookCollectGarbage = h }(testHookCollectGarbage)

	// expected value for the unchanged function
	original := 1
	// expected value for the changed function
	changed := 2

	// this variable will be set with two different functions
	var got int

	// define the original (unchanged) functions
	testHookCollectGarbage = func(_ uint64) {
		got = original
	}

	// set got variable
	testHookCollectGarbage(0)

	// test if got variable is set correctly
	if got != original {
		t.Errorf("got hook value %v, want %v", got, original)
	}

	// set the new function
	reset := setTestHook(&testHookCollectGarbage, func(_ uint64) {
		got = changed
	})

	// set got variable
	testHookCollectGarbage(0)

	// test if got variable is set correctly to changed value
	if got != changed {
		t.Errorf("got hook value %v, want %v", got, changed)
	}

	// set the function to the original one
	reset()

	// set got variable
	testHookCollectGarbage(0)

	// test if got variable is set correctly to original value
	if got != original {
		t.Errorf("got hook value %v, want %v", got, original)
	}
}

func TestPinAfterMultiGC(t *testing.T) {
	db := newTestDB(t, &Options{
		Capacity: 10,
	})

	pinnedChunks := make([]boson.Address, 0)

	// upload random chunks above cache capacity to see if chunks are still pinned
	for i := 0; i < 20; i++ {
		ch := generateTestRandomChunk()
		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}

		if len(pinnedChunks) < 10 {
			rch := generateAndPinAChunk(t, db)
			pinnedChunks = append(pinnedChunks, rch.Address())
		}
	}
	for i := 0; i < 20; i++ {
		ch := generateTestRandomChunk()
		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		ch := generateTestRandomChunk()
		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("pin Index count", newItemsCountTest(db.pinIndex, len(pinnedChunks)))

	// Check if all the pinned chunks are present in the data DB
	for _, addr := range pinnedChunks {
		outItem := shed.Item{
			Address: addr.Bytes(),
		}
		ctx := sctx.SetRootCID(context.Background(), addr)
		gotChunk, err := db.Get(ctx, storage.ModeGetRequest, boson.NewAddress(outItem.Address))
		if err != nil {
			t.Fatal(err)
		}
		if !gotChunk.Address().Equal(boson.NewAddress(addr.Bytes())) {
			t.Fatal("Pinned chunk is not equal to got chunk")
		}
	}

}

func generateAndPinAChunk(t *testing.T, db *DB) boson.Chunk {
	// Create a chunk and pin it
	pinnedChunk := generateTestRandomChunk()

	_, err := db.Put(context.Background(), storage.ModePutUpload, pinnedChunk)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Set(context.Background(), storage.ModeSetPin, pinnedChunk.Address())
	if err != nil {
		t.Fatal(err)
	}
	err = db.Set(context.Background(), storage.ModeSetSync, pinnedChunk.Address())
	if err != nil {
		t.Fatal(err)
	}
	return pinnedChunk
}

func TestPinSyncAndAccessPutSetChunkMultipleTimes(t *testing.T) {
	var closed chan struct{}
	testHookCollectGarbageChan := make(chan uint64)
	t.Cleanup(setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		select {
		case testHookCollectGarbageChan <- collectedCount:
		case <-closed:
		}
	}))
	db := newTestDB(t, &Options{
		Capacity: 10,
	})
	closed = db.close

	pinnedChunks := addRandomChunks(t, 5, db, true)
	rand1Chunks := addRandomChunks(t, 15, db, false)
	for _, ch := range pinnedChunks {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, ch.Address())
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, ch := range rand1Chunks {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, ch.Address())
		if err != nil {
			// ignore the chunks that are GCd
			continue
		}
	}

	rand2Chunks := addRandomChunks(t, 20, db, false)
	for _, ch := range rand2Chunks {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, ch.Address())
		if err != nil {
			// ignore the chunks that are GCd
			continue
		}
	}

	rand3Chunks := addRandomChunks(t, 20, db, false)

	for _, ch := range rand3Chunks {
		_, err := db.Get(context.Background(), storage.ModeGetRequest, ch.Address())
		if err != nil {
			// ignore the chunks that are GCd
			continue
		}
	}

	// check if the pinned chunk is present after GC
	for _, ch := range pinnedChunks {
		gotChunk, err := db.Get(context.Background(), storage.ModeGetRequest, ch.Address())
		if err != nil {
			t.Fatal("Pinned chunk missing ", err)
		}
		if !gotChunk.Address().Equal(ch.Address()) {
			t.Fatal("Pinned chunk address is not equal to got chunk")
		}

		if !bytes.Equal(gotChunk.Data(), ch.Data()) {
			t.Fatal("Pinned chunk data is not equal to got chunk")
		}
	}

}

func addRandomChunks(t *testing.T, count int, db *DB, pin bool) []boson.Chunk {
	var chunks []boson.Chunk
	for i := 0; i < count; i++ {
		ch := generateTestRandomChunk()
		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}
		if pin {
			err = db.Set(ctx, storage.ModeSetPin, ch.Address())
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Get(ctx, storage.ModeGetRequest, ch.Address())
			if err != nil {
				t.Fatal(err)
			}
		} else {
			// Non pinned chunks could be GC'd by the time they reach here.
			// so it is okay to ignore the error
			_, _ = db.Get(ctx, storage.ModeGetRequest, ch.Address())
		}
		chunks = append(chunks, ch)
	}
	return chunks
}

func addRandomFile(t *testing.T, count int, db *DB, ci *chunkinfo.ChunkInfo, pin bool) (reference boson.Address, chunkHashes []boson.Address, addGc func(*testing.T)) {
	buf := new(bytes.Buffer)

	mode := storage.ModePutRequest
	if pin {
		mode = storage.ModePutRequestPin
	}

	for i := 0; i < count; i++ {
		ch := generateTestRandomChunk()
		buf.Write(ch.Data()[boson.SpanSize:])
	}

	ctx := context.Background()

	pipe := builder.NewPipelineBuilder(ctx, db, mode, false)
	fr, err := builder.FeedPipeline(ctx, pipe, buf)
	if err != nil {
		t.Fatal(err)
	}

	ls := loadsave.New(db, func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, db, mode, false)
	})
	m, err := manifest.NewDefaultManifest(ls, false)
	if err != nil {
		t.Fatal(err)
	}

	fileMtdt := map[string]string{
		manifest.EntryMetadataFilenameKey:    fr.String(),
		manifest.EntryMetadataContentTypeKey: "text/plain; charset=utf-8",
	}
	err = m.Add(ctx, fr.String(), manifest.NewEntry(fr, fileMtdt))
	if err != nil {
		t.Fatal(err)
	}
	reference, err = m.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}

	item := addressToItem(reference)
	data, err := db.retrievalDataIndex.Get(item)
	if err != nil {
		t.Fatal(err)
	}

	traverser := traversal.New(db)
	err = traverser.Traverse(ctx, reference, func(address boson.Address) error {
		chunkHashes = append(chunkHashes, address)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, chunk := range chunkHashes {
		ci.PutChunkPyramid(reference, chunk, i)
	}
	if !pin {
		addGc = func(t2 *testing.T) {
			t2.Helper()
			exists, err := db.pinIndex.Has(item)
			if err != nil {
				t2.Fatal(err)
			}
			if exists {
				return
			}
			item.AccessTimestamp = now()
			err = db.retrievalAccessIndex.Put(item)
			if err != nil {
				t2.Fatal(err)
			}
			item.BinID = data.BinID
			item.GCounter = uint64(len(chunkHashes))
			err = db.gcIndex.Put(item)
			if err != nil {
				t2.Fatal(err)
			}
			curSize, err := db.gcSize.Get()
			if err != nil {
				t2.Fatal(err)
			}
			err = db.gcSize.Put(curSize + uint64(len(chunkHashes)))
			if err != nil {
				t2.Fatal(err)
			}
		}
	}
	return
}

// TestGC_NoEvictDirty checks that the garbage collection
// does not evict chunks that are marked as dirty while the gc
// is running.
func TestGC_NoEvictDirty(t *testing.T) {
	// lower the maximal number of chunks in a single
	// gc batch to ensure multiple batches.
	defer func(s uint64) { gcBatchSize = s }(gcBatchSize)
	gcBatchSize = 1

	chunkCount := 10

	db := newTestDB(t, &Options{
		Capacity: 10,
	})

	testHookCollectGarbageChan := make(chan uint64)
	t.Cleanup(setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		// don't trigger if we haven't collected anything - this may
		// result in a race condition when we inspect the gcsize below,
		// causing the database to shut down while the cleanup to happen
		// before the correct signal has been communicated here.
		if collectedCount == 0 {
			return
		}
		select {
		case testHookCollectGarbageChan <- collectedCount:
		case <-db.close:
		}
	}))

	dirtyChan := make(chan struct{})
	incomingChan := make(chan struct{})
	t.Cleanup(setTestHookGCIteratorDone(func() {
		incomingChan <- struct{}{}
		<-dirtyChan
	}))
	defer close(incomingChan)
	addrs := make([]boson.Address, 0)
	mtx := new(sync.Mutex)
	online := make(chan struct{})
	go func() {
		close(online) // make sure this is scheduled, otherwise test might flake
		i := 0
		for range incomingChan {
			// set a chunk to be updated in gc, resulting
			// in a removal from the gc round. but don't do this
			// for all chunks!
			if i < 2 {
				mtx.Lock()
				ctx := sctx.SetRootCID(context.Background(), addrs[i])
				_, err := db.Get(ctx, storage.ModeGetRequest, addrs[i])
				mtx.Unlock()
				if err != nil {
					t.Error(err)
				}
				i++
				// we sleep so that the async update to gc index
				// happens and that the dirtyAddresses get updated
				time.Sleep(100 * time.Millisecond)
			}
			dirtyChan <- struct{}{}
		}
	}()
	<-online
	// upload random chunks
	for i := 0; i < chunkCount; i++ {
		ch := generateTestRandomChunk()
		ctx := sctx.SetRootCID(context.Background(), ch.Address())
		_, err := db.Put(ctx, storage.ModePutRequest, ch)
		if err != nil {
			t.Fatal(err)
		}

		mtx.Lock()
		addrs = append(addrs, ch.Address())
		mtx.Unlock()
	}

	gcTarget := db.gcTarget()
	for {
		select {
		case <-testHookCollectGarbageChan:
		case <-time.After(10 * time.Second):
			t.Error("collect garbage timeout")
		}
		gcSize, err := db.gcSize.Get()
		if err != nil {
			t.Fatal(err)
		}
		if gcSize == gcTarget {
			break
		}
	}

	t.Run("gc index count", newItemsCountTest(db.gcIndex, int(gcTarget)))

	t.Run("gc size", newIndexGCSizeTest(db))

	// the first synced chunk should be removed
	t.Run("get the first two chunks, third is gone", func(t *testing.T) {
		_, err := db.Get(sctx.SetRootCID(context.Background(), addrs[0]), storage.ModeGetRequest, addrs[0])
		if err != nil {
			t.Error("got error but expected none")
		}
		_, err = db.Get(sctx.SetRootCID(context.Background(), addrs[1]), storage.ModeGetRequest, addrs[1])
		if err != nil {
			t.Error("got error but expected none")
		}
		_, err = db.Get(sctx.SetRootCID(context.Background(), addrs[2]), storage.ModeGetRequest, addrs[2])
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected err not found but got %v", err)
		}
	})

	t.Run("only later inserted chunks should be removed", func(t *testing.T) {
		for i := 2; i < (chunkCount - int(gcTarget)); i++ {
			_, err := db.Get(sctx.SetRootCID(context.Background(), addrs[i]), storage.ModeGetRequest, addrs[i])
			if !errors.Is(err, storage.ErrNotFound) {
				t.Errorf("got error %v, want %v", err, storage.ErrNotFound)
			}
		}
	})

	// last synced chunk should not be removed
	t.Run("get most recent synced chunk", func(t *testing.T) {
		_, err := db.Get(sctx.SetRootCID(context.Background(), addrs[len(addrs)-1]), storage.ModeGetRequest, addrs[len(addrs)-1])
		if err != nil {
			t.Fatal(err)
		}
	})

}

// setTestHookGCIteratorDone sets testHookGCIteratorDone and
// returns a function that will reset it to the
// value before the change.
func setTestHookGCIteratorDone(h func()) (reset func()) {
	current := testHookGCIteratorDone
	reset = func() { testHookGCIteratorDone = current }
	testHookGCIteratorDone = h
	return reset
}

// TestPinAndUnpinChunk checks that the garbage collection
// does not clean pinned chunks.
func TestPinAndUnpinChunk(t *testing.T) {
	chunkCount := 5

	var (
		err    error
		closed chan struct{}
	)
	testHookCollectGarbageChan := make(chan uint64)
	testHookRecycleGarbageChan := make(chan uint64)
	t.Cleanup(setTestHook(&testHookCollectGarbage, func(collectedCount uint64) {
		select {
		case testHookCollectGarbageChan <- collectedCount:
		case <-closed:
		}
	}))
	t.Cleanup(setTestHook(&testHookRecycleGarbage, func(recycledCount uint64) {
		select {
		case testHookRecycleGarbageChan <- recycledCount:
		case <-closed:
		}
	}))
	db := newTestDB(t, &Options{
		Capacity: 10,
	})
	closed = db.close

	// upload random file
	reference, chunks, addGc := addRandomFile(t, chunkCount, db, ci, false)

	for i, chunk := range chunks {
		ci.PutChunkPyramid(reference, chunk, i)
	}

	rctx := sctx.SetRootCID(context.Background(), reference)
	addGc(t)

	// pin upload files for the first time.
	for _, v := range chunks {
		err = db.Set(rctx, storage.ModeSetPin, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	chunkCount = 16
	reference1, chunks1, addGc1 := addRandomFile(t, chunkCount, db, ci, false)
	for i, chunk := range chunks1 {
		ci.PutChunkPyramid(reference1, chunk, i)
	}

	rctx = sctx.SetRootCID(context.Background(), reference1)
	for _, v := range chunks1 {
		err = db.Set(rctx, storage.ModeSetPin, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, v := range chunks1 {
		err = db.Set(rctx, storage.ModeSetUnpin, v)
		if err != nil {
			t.Fatal(err)
		}
	}
	addGc1(t)

	// trigger gc
	db.triggerGarbageCollection()
	select {
	case <-testHookRecycleGarbageChan:
	case <-time.After(10 * time.Second):
		t.Error("recycle garbage timeout")
	}

	for _, v := range chunks1 {
		_, err := db.Get(rctx, storage.ModeGetRequest, v)
		if err == nil {
			t.Fatal(err)
		}
	}

	// check whether the pin file exists.
	for _, v := range chunks {
		_, err := db.Get(rctx, storage.ModeGetRequest, v)
		if err != nil {
			t.Fatal(err)
		}
	}
}
