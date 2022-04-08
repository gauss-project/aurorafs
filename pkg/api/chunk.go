package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/netstore"
	"github.com/gauss-project/aurorafs/pkg/sctx"
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

	data, err := io.ReadAll(r.Body)
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

	_, err = s.storer.Put(ctx, requestModePut(r), chunk)
	if err != nil {
		s.logger.Debugf("chunk upload: chunk write error: %v, addr %s", err, chunk.Address())
		s.logger.Error("chunk upload: chunk write error")
		jsonhttp.BadRequest(w, "chunk write error")
		return
	}

	if strings.ToLower(r.Header.Get(AuroraPinHeader)) == StringTrue {
		if err := s.pinning.CreatePin(ctx, chunk.Address(), false); err != nil {
			s.logger.Debugf("chunk upload: creation of pin for %q failed: %v", chunk.Address(), err)
			s.logger.Error("chunk upload: creation of pin failed")
			jsonhttp.InternalServerError(w, nil)
			return
		}
	}

	jsonhttp.Created(w, chunkAddressResponse{Reference: chunk.Address()})
}

func (s *server) chunkGetHandler(w http.ResponseWriter, r *http.Request) {
	targets := r.URL.Query().Get("targets")
	if targets != "" {
		r = r.WithContext(sctx.SetTargets(r.Context(), targets))
	}

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
