package netstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/netstore"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
)

var chunkData = []byte("mockdata")

// TestNetstoreRetrieval verifies that a chunk is asked from the network whenever
// it is not found locally
func TestNetstoreRetrieval(t *testing.T) {
	retrieve, store, nstore := newRetrievingNetstore()
	addr := boson.MustParseHexAddress("000001")
	_, err := nstore.Get(context.Background(), storage.ModeGetRequest, addr)
	if err != nil {
		t.Fatal(err)
	}
	if !retrieve.called {
		t.Fatal("retrieve request not issued")
	}
	if retrieve.callCount != 1 {
		t.Fatalf("call count %d", retrieve.callCount)
	}
	if !retrieve.addr.Equal(addr) {
		t.Fatalf("addresses not equal. got %s want %s", retrieve.addr, addr)
	}

	// store should have the chunk now
	d, err := store.Get(context.Background(), storage.ModeGetRequest, addr)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(d.Data(), chunkData) {
		t.Fatal("chunk data not equal to expected data")
	}

	// check that the second call does not result in another retrieve request
	d, err = nstore.Get(context.Background(), storage.ModeGetRequest, addr)
	if err != nil {
		t.Fatal(err)
	}

	if retrieve.callCount != 1 {
		t.Fatalf("call count %d", retrieve.callCount)
	}
	if !bytes.Equal(d.Data(), chunkData) {
		t.Fatal("chunk data not equal to expected data")
	}

}

// TestNetstoreNoRetrieval verifies that a chunk is not requested from the network
// whenever it is found locally.
func TestNetstoreNoRetrieval(t *testing.T) {
	retrieve, store, nstore := newRetrievingNetstore()
	addr := boson.MustParseHexAddress("000001")

	// store should have the chunk in advance
	_, err := store.Put(context.Background(), storage.ModePutUpload, boson.NewChunk(addr, chunkData))
	if err != nil {
		t.Fatal(err)
	}

	c, err := nstore.Get(context.Background(), storage.ModeGetRequest, addr)
	if err != nil {
		t.Fatal(err)
	}
	if retrieve.called {
		t.Fatal("retrieve request issued but shouldn't")
	}
	if retrieve.callCount != 0 {
		t.Fatalf("call count %d", retrieve.callCount)
	}
	if !bytes.Equal(c.Data(), chunkData) {
		t.Fatal("chunk data mismatch")
	}
}

func TestRecovery(t *testing.T) {
	callbackWasCalled := make(chan bool, 1)

	retrieve, _, nstore := newRetrievingNetstore()
	addr := boson.MustParseHexAddress("deadbeef")
	retrieve.failure = true
	ctx := context.Background()

	_, err := nstore.Get(ctx, storage.ModeGetRequest, addr)
	if err != nil && !errors.Is(err, netstore.ErrRecoveryAttempt) {
		t.Fatal(err)
	}

	select {
	case <-callbackWasCalled:
		break
	case <-time.After(100 * time.Millisecond):
		t.Fatal("recovery callback was not called")
	}
}

func TestInvalidRecoveryFunction(t *testing.T) {
	retrieve, _, nstore := newRetrievingNetstore()
	addr := boson.MustParseHexAddress("deadbeef")
	retrieve.failure = true
	ctx := context.Background()

	_, err := nstore.Get(ctx, storage.ModeGetRequest, addr)
	if err != nil && err.Error() != "chunk not found" {
		t.Fatal(err)
	}
}

// returns a mock retrieval protocol, a mock local storage and a netstore
func newRetrievingNetstore() (ret *retrievalMock, mockStore, ns storage.Storer) {
	retrieve := &retrievalMock{}
	store := mock.NewStorer()
	logger := logging.New(io.Discard, 0)
	return retrieve, store, netstore.New(store, retrieve, logger)
}

type retrievalMock struct {
	called    bool
	callCount int32
	failure   bool
	addr      boson.Address
}

func (r *retrievalMock) GetRouteScore(time int64) map[string]int64 {
	panic("implement me")
}

func (r *retrievalMock) RetrieveChunk(ctx context.Context, rootAddr, addr boson.Address) (chunk boson.Chunk, err error) {
	if r.failure {
		return nil, fmt.Errorf("chunk not found")
	}
	r.called = true
	atomic.AddInt32(&r.callCount, 1)
	r.addr = addr
	return boson.NewChunk(addr, chunkData), nil
}

//type mockRecovery struct {
//	callbackC chan bool
//}
//
//func (r *mockRecovery) RetrieveChunk(ctx context.Context, addr boson.Address) (chunk boson.Chunk, err error) {
//	return nil, fmt.Errorf("chunk not found")
//}
