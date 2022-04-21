package addresses_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/addresses"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	filetest "github.com/gauss-project/aurorafs/pkg/file/testing"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
)

func TestAddressesGetterIterateChunkAddresses(t *testing.T) {
	store := mock.NewStorer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// create root chunk with 2 references and the referenced data chunks
	rootChunk := filetest.GenerateTestRandomFileChunk(boson.ZeroAddress, boson.ChunkSize*2, boson.SectionSize*2)
	_, err := store.Put(ctx, storage.ModePutUpload, rootChunk)
	if err != nil {
		t.Fatal(err)
	}

	firstAddress := boson.NewAddress(rootChunk.Data()[8 : boson.SectionSize+8])
	firstChunk := filetest.GenerateTestRandomFileChunk(firstAddress, boson.ChunkSize, boson.ChunkSize)
	_, err = store.Put(ctx, storage.ModePutUpload, firstChunk)
	if err != nil {
		t.Fatal(err)
	}

	secondAddress := boson.NewAddress(rootChunk.Data()[boson.SectionSize+8:])
	secondChunk := filetest.GenerateTestRandomFileChunk(secondAddress, boson.ChunkSize, boson.ChunkSize)
	_, err = store.Put(ctx, storage.ModePutUpload, secondChunk)
	if err != nil {
		t.Fatal(err)
	}

	createdAddresses := []boson.Address{rootChunk.Address(), firstAddress, secondAddress}

	foundAddresses := make(map[string]struct{})
	var foundAddressesMu sync.Mutex

	addressIterFunc := func(addr boson.Address) error {
		foundAddressesMu.Lock()
		defer foundAddressesMu.Unlock()

		foundAddresses[addr.String()] = struct{}{}
		return nil
	}

	addressesGetter := addresses.NewGetter(store, addressIterFunc)

	j, _, err := joiner.New(ctx, addressesGetter, storage.ModeGetRequest, rootChunk.Address())
	if err != nil {
		t.Fatal(err)
	}

	_, err = file.JoinReadAll(ctx, j, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if len(createdAddresses) != len(foundAddresses) {
		t.Fatalf("expected to find %d addresses, got %d", len(createdAddresses), len(foundAddresses))
	}

	checkAddressFound := func(t *testing.T, foundAddresses map[string]struct{}, address boson.Address) {
		t.Helper()

		if _, ok := foundAddresses[address.String()]; !ok {
			t.Fatalf("expected address %s not found", address.String())
		}
	}

	for _, createdAddress := range createdAddresses {
		checkAddressFound(t, foundAddresses, createdAddress)
	}
}
