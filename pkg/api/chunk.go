// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/netstore"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/storage"

	"github.com/gorilla/mux"
)

type chunkAddressResponse struct {
	Reference boson.Address `json:"reference"`
}

func (s *server) chunkUploadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		ctx = r.Context()
		err error
	)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		if jsonhttp.HandleBodyReadError(err, w) {
			return
		}
		s.logger.Debugf("chunk upload: read chunk data error: %v", err)
		s.logger.Error("chunk upload: read chunk data error")
		jsonhttp.InternalServerError(w, "cannot read chunk data")
		return
	}

	if len(data) < boson.SpanSize {
		s.logger.Debug("chunk upload: not enough data")
		s.logger.Error("chunk upload: data length")
		jsonhttp.BadRequest(w, "data length")
		return
	}

	chunk, err := cac.NewWithDataSpan(data)
	if err != nil {
		s.logger.Debugf("chunk upload: create chunk error: %v", err)
		s.logger.Error("chunk upload: create chunk error")
		jsonhttp.InternalServerError(w, "create chunk error")
		return
	}

	seen, err := s.storer.Put(ctx, requestModePut(r), chunk)
	if err != nil {
		s.logger.Debugf("chunk upload: chunk write error: %v, addr %s", err, chunk.Address())
		s.logger.Error("chunk upload: chunk write error")
		jsonhttp.BadRequest(w, "chunk write error")
		return
	} else if len(seen) > 0 && seen[0] {

		s.logger.Debugf("chunk upload: increment tag", err)
		s.logger.Error("chunk upload: increment tag")
		jsonhttp.BadRequest(w, "increment tag")
		return
	}

	jsonhttp.OK(w, chunkAddressResponse{Reference: chunk.Address()})
}

func (s *server) chunkGetHandler(w http.ResponseWriter, r *http.Request) {
	targets := r.URL.Query().Get("targets")

	nameOrHex := mux.Vars(r)["addr"]
	ctx := r.Context()

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		s.logger.Debugf("chunk: parse chunk address %s: %v", nameOrHex, err)
		s.logger.Error("chunk: parse chunk address error")
		jsonhttp.NotFound(w, nil)
		return
	}

	chunk, err := s.storer.Get(ctx, storage.ModeGetRequest, address)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.logger.Tracef("chunk: chunk not found. addr %s", address)
			jsonhttp.NotFound(w, "chunk not found")
			return

		}
		if errors.Is(err, netstore.ErrRecoveryAttempt) {
			s.logger.Tracef("chunk: chunk recovery initiated. addr %s", address)
			jsonhttp.Accepted(w, "chunk recovery initiated. retry after sometime.")
			return
		}
		s.logger.Debugf("chunk: chunk read error: %v ,addr %s", err, address)
		s.logger.Error("chunk: chunk read error")
		jsonhttp.InternalServerError(w, "chunk read error")
		return
	}
	w.Header().Set("Content-Type", "binary/octet-stream")
	if targets != "" {
		w.Header().Set(TargetsRecoveryHeader, targets)
	}
	_, _ = io.Copy(w, bytes.NewReader(chunk.Data()))
}

// deleteChunkFn return iterator but skip reference hash.
func (s *server) deleteChunkFn(ctx context.Context, reference boson.Address) func(address boson.Address) error {
	return func(address boson.Address) error {
		if !address.Equal(reference) {
			err := s.storer.Set(ctx, storage.ModeSetRemove, address)
			if err != nil {
				s.logger.Debugf("del traversal: for reference %s, address %s: %w", reference, address, err)
				// continue un-pinning all chunks
			}

		}
		return nil
	}
}
