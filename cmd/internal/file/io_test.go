package file_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	cmdfile "github.com/gauss-project/aurorafs/cmd/internal/file"
	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	testingc "github.com/gauss-project/aurorafs/pkg/storage/testing"
)

const (
	hashOfFoo = "2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48"
)

// TestApiStore verifies that the api store layer does not distort data, and that same
// data successfully posted can be retrieved from http backend.
func TestApiStore(t *testing.T) {
	storer := mock.NewStorer()
	ctx := context.Background()
	srvUrl := newTestServer(t, storer)

	host := srvUrl.Hostname()
	port, err := strconv.Atoi(srvUrl.Port())
	if err != nil {
		t.Fatal(err)
	}
	a := cmdfile.NewApiStore(host, port, false)

	ch := testingc.GenerateTestRandomChunk()
	_, err = a.Put(ctx, storage.ModePutUpload, ch)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storer.Get(ctx, storage.ModeGetRequest, ch.Address())
	if err != nil {
		t.Fatal(err)
	}
	chResult, err := a.Get(ctx, storage.ModeGetRequest, ch.Address())
	if err != nil {
		t.Fatal(err)
	}
	if !ch.Equal(chResult) {
		t.Fatal("chunk mismatch")
	}
}

// TestFsStore verifies that the fs store layer does not distort data, and that the
// resulting stored data matches what is submitted.
func TestFsStore(t *testing.T) {
	tmpPath, err := ioutil.TempDir("", "cli-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpPath)
	storer := cmdfile.NewFsStore(tmpPath)
	chunkAddr := boson.MustParseHexAddress(hashOfFoo)
	chunkData := []byte("foo")
	ch := boson.NewChunk(chunkAddr, chunkData)

	ctx := context.Background()
	_, err = storer.Put(ctx, storage.ModePutUpload, ch)
	if err != nil {
		t.Fatal(err)
	}

	chunkFilename := filepath.Join(tmpPath, hashOfFoo)
	chunkDataResult, err := ioutil.ReadFile(chunkFilename)
	if err != nil {
		t.Fatal(err)
	}

	chResult := boson.NewChunk(chunkAddr, chunkDataResult)
	if !ch.Equal(chResult) {
		t.Fatal("chunk mismatch")
	}
}

// TestTeeStore verifies that the TeeStore writes to all added stores.
func TestTeeStore(t *testing.T) {
	storeFee := mock.NewStorer()
	storeFi := mock.NewStorer()
	storeFo := mock.NewStorer()
	storeFum := cmdfile.NewTeeStore()
	storeFum.Add(storeFee)
	storeFum.Add(storeFi)
	storeFum.Add(storeFo)

	chunkAddr := boson.MustParseHexAddress(hashOfFoo)
	chunkData := []byte("foo")
	ch := boson.NewChunk(chunkAddr, chunkData)

	ctx := context.Background()
	var err error
	_, err = storeFum.Put(ctx, storage.ModePutUpload, ch)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storeFee.Get(ctx, storage.ModeGetRequest, chunkAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storeFi.Get(ctx, storage.ModeGetRequest, chunkAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storeFo.Get(ctx, storage.ModeGetRequest, chunkAddr)
	if err != nil {
		t.Fatal(err)
	}
}

// TestLimitWriter verifies that writing will fail when capacity is exceeded.
func TestLimitWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	data := []byte("foo")
	writeCloser := cmdfile.NopWriteCloser(buf)
	w := cmdfile.NewLimitWriteCloser(writeCloser, int64(len(data)))
	c, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if c < 3 {
		t.Fatal("short write")
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("expected written data %x, got %x", data, buf.Bytes())
	}
	_, err = w.Write(data[:1])
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

// newTestServer creates an http server to serve the aurorafs http api endpoints.
func newTestServer(t *testing.T, storer storage.Storer) *url.URL {
	t.Helper()
	logger := logging.New(ioutil.Discard, 0)
	//store := statestore.NewStateStore()
	s := api.New(storer, nil, nil, logger, nil, api.Options{})
	ts := httptest.NewServer(s)
	srvUrl, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	return srvUrl
}
