package api

import (
	"errors"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/gorilla/mux"
	"github.com/syndtr/goleveldb/leveldb"
)

func (s *server) pinAuroras(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("pin aurora: parse address %s: %v", addr, err)
		s.logger.Errorf("pin aurora: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	// MUST request local db
	r = r.WithContext(sctx.SetRootCID(sctx.SetLocalGet(r.Context()), hash))

	pin, err := s.storer.Has(r.Context(), storage.ModeHasPin, hash)
	if err != nil {
		s.logger.Debugf("pin aurora: check %s pin: %v", hash, err)
		s.logger.Error("pin aurora: check %s pin", hash)
		jsonhttp.InternalServerError(w, err)
		return
	}
	if pin {
		jsonhttp.BadRequest(w, "dirs has pinned")
		return
	}

	addresses := make([]boson.Address, 0)

	err = s.traversal.TraverseManifestAddresses(r.Context(), hash, func(address boson.Address) error {
		addresses = append(addresses, address)
		return nil
	})
	if err != nil {
		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Errorf("pin aurora: : for reference %s", hash)
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Debugf("pin aurora: traverse chunks: for reference %s: %v", hash, err)
		s.logger.Errorf("pin aurora: traverse chunks: for reference %s", hash)
		jsonhttp.InternalServerError(w, "dirs download not completed")
		return
	}

	for _, addr := range addresses {
		err = s.storer.Set(r.Context(), storage.ModeSetPin, addr)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				continue
			}

			s.logger.Debugf("pin aurora: set pin: for reference %s, address %s: %v", hash, addr, err)
			s.logger.Errorf("pin aurora: set pin: for reference %s, address %s", hash, addr)
			jsonhttp.InternalServerError(w, "dirs pin failed")
		}
	}

	jsonhttp.OK(w, nil)
}

func (s *server) unpinAuroras(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("unpin aurora: parse address %s: %v", addr, err)
		s.logger.Errorf("unpin aurora: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	// MUST request local db
	r = r.WithContext(sctx.SetRootCID(sctx.SetLocalGet(r.Context()), hash))

	pin, err := s.storer.Has(r.Context(), storage.ModeHasPin, hash)
	if err != nil {
		s.logger.Debugf("unpin aurora: check %s unpin: %v", hash, err)
		s.logger.Errorf("unpin aurora: check %s unpin: %v", hash)
		jsonhttp.InternalServerError(w, err)
		return
	}
	if !pin {
		jsonhttp.BadRequest(w, "dirs has unpinned")
		return
	}

	chunkAddressFn := s.unpinChunkAddressFn(r.Context(), hash)

	err = s.traversal.TraverseManifestAddresses(r.Context(), hash, chunkAddressFn)
	if err != nil {
		if errors.Is(err, traversal.ErrInvalidType) {
			s.logger.Errorf("unpin aurora: for reference %s", hash)
			jsonhttp.BadRequest(w, "invalid type")
			return
		}

		s.logger.Debugf("unpin aurora: traverse chunks: for reference %s: %v", hash, err)
		s.logger.Errorf("unpin aurora: traverse chunks: for reference %s: %v", hash)
		jsonhttp.InternalServerError(w, "dirs unpin failed")
		return
	}

	jsonhttp.OK(w, nil)
}
