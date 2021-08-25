// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
)

type bytesPostResponse struct {
	Reference boson.Address `json:"reference"`
}

// bytesUploadHandler handles upload of raw binary data of arbitrary length.
func (s *server) bytesUploadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)

	// Add the tag to the context
	ctx := r.Context()

	pipe := builder.NewPipelineBuilder(ctx, s.storer, requestModePut(r), requestEncrypt(r))
	address, err := builder.FeedPipeline(ctx, pipe, r.Body, r.ContentLength)
	if err != nil {
		logger.Debugf("bytes upload: split write all: %v", err)
		logger.Error("bytes upload: split write all")
		jsonhttp.InternalServerError(w, nil)
		return
	}

	a, err := s.traversal.GetTrieData(ctx, address)
	if err != nil {
		logger.Errorf("bytes upload: get trie data err: %v", err)
		jsonhttp.InternalServerError(w, "could not get trie data")
		return
	}
	dataChunks ,_ := s.traversal.CheckTrieData(ctx, address, a)
	if err != nil {
		logger.Errorf("bytes upload: check trie data err: %v", err)
		jsonhttp.InternalServerError(w, "check trie data error")
		return
	}
	for _,li := range dataChunks {
		for _,b := range li {
			s.chunkInfo.OnChunkTransferred(boson.NewAddress(b), address, s.overlay)
		}
	}

	jsonhttp.OK(w, bytesPostResponse{
		Reference: address,
	})
}

// bytesGetHandler handles retrieval of raw binary data of arbitrary length.
func (s *server) bytesGetHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger).Logger
	nameOrHex := mux.Vars(r)["address"]

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("bytes: parse address %s: %v", nameOrHex, err)
		logger.Error("bytes: parse address error")
		jsonhttp.NotFound(w, nil)
		return
	}

	if !s.chunkInfo.Init(r.Context(), nil, address) {
		logger.Debugf("bytes get: chunkInfo init %s: %v", nameOrHex, err)
		jsonhttp.NotFound(w, nil)
		return
	}

	additionalHeaders := http.Header{
		"Content-Type": {"application/octet-stream"},
	}

	s.downloadHandler(w, r, address, additionalHeaders, true)
}
