package store_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	mock "github.com/gauss-project/aurorafs/pkg/file/pipeline/mock"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/store"
	"github.com/gauss-project/aurorafs/pkg/storage"
	storer "github.com/gauss-project/aurorafs/pkg/storage/mock"
)

// TestStoreWriter tests that store writer stores the provided data and calls the next chain writer.
func TestStoreWriter(t *testing.T) {
	mockStore := storer.NewStorer()
	mockChainWriter := mock.NewChainWriter()
	ctx := context.Background()
	writer := store.NewStoreWriter(ctx, mockStore, storage.ModePutUpload, mockChainWriter)

	for _, tc := range []struct {
		name   string
		ref    []byte
		data   []byte
		expErr error
	}{
		{
			name:   "no data",
			expErr: store.ErrInvalidData,
		},
		{
			name: "some data",
			ref:  []byte{0xaa, 0xbb, 0xcc},
			data: []byte("hello world"),
		},
		{},
	} {
		args := pipeline.PipeWriteArgs{Ref: tc.ref, Data: tc.data}
		err := writer.ChainWrite(&args)

		if err != nil && tc.expErr != nil && errors.Is(err, tc.expErr) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}

		d, err := mockStore.Get(ctx, storage.ModeGetRequest, boson.NewAddress(tc.ref))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tc.data, d.Data()) {
			t.Fatal("data mismatch")
		}
		if calls := mockChainWriter.ChainWriteCalls(); calls != 1 {
			t.Errorf("wanted 1 ChainWrite call, got %d", calls)
		}
	}
}

// TestSum tests that calling Sum on the store writer results in Sum on the next writer in the chain.
func TestSum(t *testing.T) {
	mockChainWriter := mock.NewChainWriter()
	ctx := context.Background()
	writer := store.NewStoreWriter(ctx, nil, storage.ModePutUpload, mockChainWriter)
	_, err := writer.Sum()
	if err != nil {
		t.Fatal(err)
	}
	if calls := mockChainWriter.SumCalls(); calls != 1 {
		t.Fatalf("wanted 1 Sum call but got %d", calls)
	}
}
