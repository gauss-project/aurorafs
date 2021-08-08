// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package soc_test

import (
	"strings"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/soc"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

// TestValid verifies that the validator can detect
// valid soc chunks.
func TestValid(t *testing.T) {
	socAddress := boson.MustParseHexAddress("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85")

	// signed soc chunk of:
	// id: 0
	// wrapped chunk of: `foo`
	// owner: 0x8d3766440f0d7b949a5e32995d09619a7f86e632
	sch := boson.NewChunk(socAddress, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 205, 56, 79, 235, 193, 51, 183, 178, 69, 229, 221, 198, 45, 130, 210, 205, 237, 145, 130, 210, 113, 97, 38, 205, 136, 68, 80, 154, 246, 90, 5, 61, 235, 65, 130, 8, 2, 127, 84, 142, 62, 136, 52, 58, 246, 248, 74, 135, 114, 251, 60, 235, 192, 161, 131, 58, 14, 167, 236, 12, 19, 72, 49, 27, 3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111})

	// check valid chunk
	if !soc.Valid(sch) {
		t.Fatal("valid chunk evaluates to invalid")
	}
}

// TestInvalid verifies that the validator can detect chunks
// with invalid data and invalid address.
func TestInvalid(t *testing.T) {
	socAddress := boson.MustParseHexAddress("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85")

	// signed soc chunk of:
	// id: 0
	// wrapped chunk of: `foo`
	// owner: 0x8d3766440f0d7b949a5e32995d09619a7f86e632
	sch := boson.NewChunk(socAddress, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 205, 56, 79, 235, 193, 51, 183, 178, 69, 229, 221, 198, 45, 130, 210, 205, 237, 145, 130, 210, 113, 97, 38, 205, 136, 68, 80, 154, 246, 90, 5, 61, 235, 65, 130, 8, 2, 127, 84, 142, 62, 136, 52, 58, 246, 248, 74, 135, 114, 251, 60, 235, 192, 161, 131, 58, 14, 167, 236, 12, 19, 72, 49, 27, 3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111})

	for _, c := range []struct {
		name  string
		chunk func() boson.Chunk
	}{
		{
			name: "wrong soc address",
			chunk: func() boson.Chunk {
				wrongAddressBytes := sch.Address().Bytes()
				wrongAddressBytes[0] = 255 - wrongAddressBytes[0]
				wrongAddress := boson.NewAddress(wrongAddressBytes)
				return boson.NewChunk(wrongAddress, sch.Data())
			},
		},
		{
			name: "invalid data",
			chunk: func() boson.Chunk {
				data := make([]byte, len(sch.Data()))
				copy(data, sch.Data())
				cursor := soc.IdSize + soc.SignatureSize
				chunkData := data[cursor:]
				chunkData[0] = 0x01
				return boson.NewChunk(socAddress, data)
			},
		},
		{
			name: "invalid id",
			chunk: func() boson.Chunk {
				data := make([]byte, len(sch.Data()))
				copy(data, sch.Data())
				id := data[:soc.IdSize]
				id[0] = 0x01
				return boson.NewChunk(socAddress, data)
			},
		},
		{
			name: "invalid signature",
			chunk: func() boson.Chunk {
				data := make([]byte, len(sch.Data()))
				copy(data, sch.Data())
				// modify signature
				cursor := soc.IdSize + soc.SignatureSize
				sig := data[soc.IdSize:cursor]
				sig[0] = 0x01
				return boson.NewChunk(socAddress, data)
			},
		},
		{
			name: "nil data",
			chunk: func() boson.Chunk {
				return boson.NewChunk(socAddress, nil)
			},
		},
		{
			name: "small data",
			chunk: func() boson.Chunk {
				return boson.NewChunk(socAddress, []byte("small"))
			},
		},
		{
			name: "large data",
			chunk: func() boson.Chunk {
				return boson.NewChunk(socAddress, []byte(strings.Repeat("a", boson.ChunkSize+boson.SpanSize+1)))
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if soc.Valid(c.chunk()) {
				t.Fatal("chunk with invalid data evaluates to valid")
			}
		})
	}
}
