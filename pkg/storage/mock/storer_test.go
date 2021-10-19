package mock_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
)

func TestMockStorer(t *testing.T) {
	s := mock.NewStorer()

	keyFound, err := boson.ParseHexAddress("aabbcc")
	if err != nil {
		t.Fatal(err)
	}
	keyNotFound, err := boson.ParseHexAddress("bbccdd")
	if err != nil {
		t.Fatal(err)
	}

	valueFound := []byte("data data data")

	ctx := context.Background()
	if _, err := s.Get(ctx, storage.ModeGetRequest, keyFound); err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if _, err := s.Get(ctx, storage.ModeGetRequest, keyNotFound); err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if _, err := s.Put(ctx, storage.ModePutUpload, boson.NewChunk(keyFound, valueFound)); err != nil {
		t.Fatalf("expected not error but got: %v", err)
	}

	if chunk, err := s.Get(ctx, storage.ModeGetRequest, keyFound); err != nil {
		t.Fatalf("expected no error but got: %v", err)
	} else if !bytes.Equal(chunk.Data(), valueFound) {
		t.Fatalf("expected value %s but got %s", valueFound, chunk.Data())
	}

	has, err := s.Has(ctx, storage.ModeHasChunk, keyFound)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected mock store to have key")
	}
}
