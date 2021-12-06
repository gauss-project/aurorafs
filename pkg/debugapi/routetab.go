package debugapi

import (
	"errors"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

type routeResponse struct {
	Paths []*routetab.Path `json:"paths"`
}

type addressResponse struct {
	Address *aurora.Address `json:"address"`
}

func (s *Service) findRouteHandel(w http.ResponseWriter, r *http.Request) {
	peerID := mux.Vars(r)["peer-id"]
	ctx := r.Context()

	span, logger, ctx := s.tracer.StartSpanFromContext(ctx, "route-api", s.logger)
	defer span.Finish()

	address, err := boson.ParseHexAddress(peerID)
	if err != nil {
		logger.Debugf("route-api: parse peer address %s: %v", peerID, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}
	route, err := s.routetab.FindRoute(ctx, address, time.Second*5)
	if err != nil {
		logger.Debugf("route-api: findroute %s: %v", peerID, err)
		jsonhttp.BadRequest(w, err)
		return
	}

	logger.Infof("route-api findroute succeeded to peer %s", peerID)
	jsonhttp.OK(w, routeResponse{
		Paths: route,
	})
}

func (s *Service) getRouteHandel(w http.ResponseWriter, r *http.Request) {
	peerID := mux.Vars(r)["peer-id"]
	ctx := r.Context()

	span, logger, ctx := s.tracer.StartSpanFromContext(ctx, "route-api", s.logger)
	defer span.Finish()

	address, err := boson.ParseHexAddress(peerID)
	if err != nil {
		logger.Debugf("route-api: parse peer address %s: %v", peerID, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}
	route, err := s.routetab.GetRoute(ctx, address)
	if err != nil {
		if errors.Is(err, routetab.ErrNotFound) {
			jsonhttp.NotFound(w, err)
			return
		}
		logger.Debugf("route-api: getroute %s: %v", peerID, err)
		jsonhttp.BadRequest(w, err)
		return
	}

	logger.Infof("route-api getroute succeeded to peer %s", peerID)
	jsonhttp.OK(w, routeResponse{
		Paths: route,
	})
}

func (s *Service) delRouteHandel(w http.ResponseWriter, r *http.Request) {
	peerID := mux.Vars(r)["peer-id"]
	ctx := r.Context()

	span, logger, ctx := s.tracer.StartSpanFromContext(ctx, "route-api", s.logger)
	defer span.Finish()

	address, err := boson.ParseHexAddress(peerID)
	if err != nil {
		logger.Debugf("route-api: parse peer address %s: %v", peerID, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}
	err = s.routetab.DelRoute(ctx, address)
	if err != nil {
		if errors.Is(err, routetab.ErrNotFound) {
			jsonhttp.NotFound(w, err)
			return
		}
		logger.Debugf("route-api: delroute %s: %v", peerID, err)
		jsonhttp.BadRequest(w, err)
		return
	}

	logger.Infof("route-api delroute succeeded to peer %s", peerID)
	jsonhttp.OK(w, nil)
}

func (s *Service) findUnderlayHandel(w http.ResponseWriter, r *http.Request) {
	peerID := mux.Vars(r)["peer-id"]
	ctx := r.Context()

	span, logger, ctx := s.tracer.StartSpanFromContext(ctx, "route-api", s.logger)
	defer span.Finish()

	address, err := boson.ParseHexAddress(peerID)
	if err != nil {
		logger.Debugf("route-api: parse peer address %s: %v", peerID, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}
	addr, err := s.routetab.FindUnderlay(ctx, address)
	if err != nil {
		if errors.Is(err, routetab.ErrNotFound) {
			jsonhttp.NotFound(w, err)
			return
		}
		logger.Debugf("route-api: FindUnderlay %s: %v", peerID, err)
		jsonhttp.BadRequest(w, err)
		return
	}

	logger.Infof("route-api FindUnderlay succeeded to peer %s", peerID)
	jsonhttp.OK(w, addressResponse{
		Address: addr,
	})
}
