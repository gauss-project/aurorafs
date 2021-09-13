package api

import (
	"errors"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/gorilla/mux"
)

func (s *server) pinFile(w http.ResponseWriter, r *http.Request) {
	addr, err := boson.ParseHexAddress(mux.Vars(r)["address"])
	if err != nil {
		s.logger.Debugf("pin files: parse address: %v", err)
		s.logger.Error("pin files: parse address")
		jsonhttp.BadRequest(w, "bad address")
		return
	}

	has, err := s.storer.Has(r.Context(), storage.ModeHasChunk, addr)
	if err != nil {
		s.logger.Debugf("pin files: localstore has: %v", err)
		s.logger.Error("pin files: store")
		jsonhttp.InternalServerError(w, err)
		return
	}

	if !has {
		_, err := s.storer.Get(r.Context(), storage.ModeGetRequest, addr)
		if err != nil {
			s.logger.Debugf("pin chunk: netstore get: %v", err)
			s.logger.Error("pin chunk: netstore")

			jsonhttp.NotFound(w, nil)
			return
		}
	}

	ctx := r.Context()

	chunkAddressFn := s.pinChunkAddressFn(ctx, addr)

	err = s.traversal.TraverseFileAddresses(ctx, addr, chunkAddressFn)
	if err != nil {
		s.logger.Debugf("pin files: traverse chunks: %v, addr %s", err, addr)

		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Error("pin files: invalid type")
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Error("pin files: cannot pin")
		jsonhttp.InternalServerError(w, "cannot pin")
		return
	}

	jsonhttp.OK(w, nil)
}

func (s *server) unpinFile(w http.ResponseWriter, r *http.Request) {
	addr, err := boson.ParseHexAddress(mux.Vars(r)["address"])
	if err != nil {
		s.logger.Debugf("pin files: parse address: %v", err)
		s.logger.Error("pin files: parse address")
		jsonhttp.BadRequest(w, "bad address")
		return
	}

	has, err := s.storer.Has(r.Context(), storage.ModeHasChunk, addr)
	if err != nil {
		s.logger.Debugf("pin files: localstore has: %v", err)
		s.logger.Error("pin files: store")
		jsonhttp.InternalServerError(w, err)
		return
	}

	if !has {
		jsonhttp.NotFound(w, nil)
		return
	}

	ctx := r.Context()

	chunkAddressFn := s.unpinChunkAddressFn(ctx, addr)

	err = s.traversal.TraverseFileAddresses(ctx, addr, chunkAddressFn)
	if err != nil {
		s.logger.Debugf("pin files: traverse chunks: %v, addr %s", err, addr)

		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Error("pin files: invalid type")
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Error("pin files: cannot unpin")
		jsonhttp.InternalServerError(w, "cannot unpin")
		return
	}

	jsonhttp.OK(w, nil)
}
