// Package file provides interfaces for file-oriented operations.
package file

import (
	"context"
	"io"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

type Reader interface {
	io.ReadSeeker
	io.ReaderAt
}

// Joiner provides the inverse functionality of the Splitter.
type Joiner interface {
	Reader
	// IterateChunkAddresses is used to iterate over chunks addresses of some root hash.
	IterateChunkAddresses(boson.AddressIterFunc) error
	// Size returns the span of the hash trie represented by the joiner's root hash.
	Size() int64

	// GetRootData returns root chunk of the given reference.
	GetRootData() []byte
	// GetDataChunks returns saved data chunk when iteration has ended.
	GetDataChunks() [][]byte
	// SetSaveDataChunks allows to obtain all chunks lying at bottom.
	SetSaveDataChunks()
	// SetSaveEdgeChunks allows to obtain all chunks that not belongs to data.
	SetSaveEdgeChunks(map[string][]byte)
}

// Splitter starts a new file splitting job.
//
// Data is read from the provided reader.
// If the dataLength parameter is 0, data is read until io.EOF is encountered.
// When EOF is received and splitting is done, the resulting Aurora Address is returned.
type Splitter interface {
	Split(ctx context.Context, dataIn io.ReadCloser, dataLength int64, toEncrypt bool) (addr boson.Address, err error)
}
