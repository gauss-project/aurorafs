package cac

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gauss-project/aurorafs/pkg/bmtpool"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	errTooShortChunkData = errors.New("short chunk data")
	errTooLargeChunkData = errors.New("data too large")
)

// New creates a new content address chunk by initializing a span and appending the data to it.
func New(data []byte) (boson.Chunk, error) {
	dataLength := len(data)
	if dataLength > boson.ChunkSize {
		return nil, errTooLargeChunkData
	}

	if dataLength == 0 {
		return nil, errTooShortChunkData
	}

	span := make([]byte, boson.SpanSize)
	binary.LittleEndian.PutUint64(span, uint64(dataLength))
	return newWithSpan(data, span)
}

// NewWithDataSpan creates a new chunk assuming that the span precedes the actual data.
func NewWithDataSpan(data []byte) (boson.Chunk, error) {
	dataLength := len(data)
	if dataLength > boson.ChunkSize+boson.SpanSize {
		return nil, errTooLargeChunkData
	}

	if dataLength < boson.SpanSize {
		return nil, errTooShortChunkData
	}
	return newWithSpan(data[boson.SpanSize:], data[:boson.SpanSize])
}

// newWithSpan creates a new chunk prepending the given span to the data.
func newWithSpan(data, span []byte) (boson.Chunk, error) {
	h := hasher(data)
	hash, err := h(span)
	if err != nil {
		return nil, err
	}

	cdata := make([]byte, len(data)+len(span))
	copy(cdata[:boson.SpanSize], span)
	copy(cdata[boson.SpanSize:], data)
	return boson.NewChunk(boson.NewAddress(hash), cdata), nil
}

// hasher is a helper function to hash a given data based on the given span.
func hasher(data []byte) func([]byte) ([]byte, error) {
	return func(span []byte) ([]byte, error) {
		hasher := bmtpool.Get()
		defer bmtpool.Put(hasher)

		if err := hasher.SetSpanBytes(span); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(data); err != nil {
			return nil, err
		}
		return hasher.Sum(nil), nil
	}
}

// Valid checks whether the given chunk is a valid content-addressed chunk.
func Valid(c boson.Chunk) bool {
	data := c.Data()
	if len(data) < boson.SpanSize {
		return false
	}

	if len(data) > boson.ChunkSize+boson.SpanSize {
		return false
	}

	h := hasher(data[boson.SpanSize:])
	hash, _ := h(data[:boson.SpanSize])
	return bytes.Equal(hash, c.Address().Bytes())
}
