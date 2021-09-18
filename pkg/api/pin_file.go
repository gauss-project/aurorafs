package api

import (
	"errors"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/gorilla/mux"
)

func (s *server) pinFile(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("pin files: parse address %s: %v", addr, err)
		s.logger.Errorf("pin files: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	// MUST request local db
	r = r.WithContext(sctx.SetRootCID(sctx.SetLocalGet(r.Context()), hash))

	_, err = s.storer.Get(r.Context(), storage.ModeGetRequest, hash)
	if err != nil {
		s.logger.Debugf("pin files: check %s exists: %v", addr, err)
		s.logger.Errorf("pin files: check %s exists", addr)

		jsonhttp.NotFound(w, nil)
		return
	}

	addresses := make([]boson.Address, 0)

	err = s.traversal.TraverseFileAddresses(r.Context(), hash, func(address boson.Address) error {
		addresses = append(addresses, address)
		return nil
	})
	if err != nil {
		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Errorf("pin files: invalid type: for reference %s", hash)
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Debugf("pin files: traverse chunks: for reference %s: %v", hash, err)
		s.logger.Errorf("pin files: traverse chunks: for reference %s", hash)
		jsonhttp.InternalServerError(w, "file download not completed")
		return
	}

	for _, addr := range addresses {
		err = s.storer.Set(r.Context(), storage.ModeSetPin, addr)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}

			s.logger.Debugf("pin files: set pin: for reference %s, address %s: %v", hash, addr, err)
			s.logger.Errorf("pin files: set pin: for reference %s, address %s", hash, addr)
			jsonhttp.InternalServerError(w, "file pin failed")
		}
	}

	jsonhttp.OK(w, nil)
}

func (s *server) unpinFile(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("unpin files: parse address %s: %v", addr, err)
		s.logger.Errorf("unpin files: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	// MUST request local db
	r = r.WithContext(sctx.SetRootCID(sctx.SetLocalGet(r.Context()), hash))

	_, err = s.storer.Get(r.Context(), storage.ModeGetRequest, hash)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			jsonhttp.NotFound(w, nil)
			return
		}

		s.logger.Debugf("unpin files: check %s exists: %v", hash, err)
		s.logger.Errorf("unpin files: check %s exists", hash)
		jsonhttp.InternalServerError(w, err)
		return
	}

	fn := s.unpinChunkAddressFn(r.Context(), hash)

	err = s.traversal.TraverseFileAddresses(r.Context(), hash, fn)
	if err != nil {
		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Errorf("unpin files: invalid type: for reference %s", hash)
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Debugf("unpin files: traverse chunks: for reference %s: %v", hash, err)
		s.logger.Errorf("unpin files: traverse chunks: for reference %s", hash)
		jsonhttp.InternalServerError(w, "file unpin failed")
		return
	}

	jsonhttp.OK(w, nil)
}
