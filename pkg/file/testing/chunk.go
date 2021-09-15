package testing

import (
	"encoding/binary"
	"math/rand"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

// GenerateTestRandomFileChunk generates one single chunk with arbitrary content and address
func GenerateTestRandomFileChunk(address boson.Address, spanLength, dataSize int) boson.Chunk {
	data := make([]byte, dataSize+8)
	binary.LittleEndian.PutUint64(data, uint64(spanLength))
	_, _ = rand.Read(data[8:]) // # skipcq: GSC-G404
	key := make([]byte, boson.SectionSize)
	if address.IsZero() {
		_, _ = rand.Read(key) // # skipcq: GSC-G404
	} else {
		copy(key, address.Bytes())
	}
	return boson.NewChunk(boson.NewAddress(key), data)
}
