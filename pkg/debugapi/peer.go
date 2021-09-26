package debugapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gorilla/mux"
	"github.com/multiformats/go-multiaddr"
)

type peerConnectResponse struct {
	Address string `json:"address"`
}

func (s *Service) peerConnectHandler(w http.ResponseWriter, r *http.Request) {
	addr, err := multiaddr.NewMultiaddr("/" + mux.Vars(r)["multi-address"])
	if err != nil {
		s.logger.Debugf("debug api: peer connect: parse multiaddress: %v", err)
		jsonhttp.BadRequest(w, err)
		return
	}

	bzzAddr, err := s.p2p.Connect(r.Context(), addr)
	if err != nil {
		s.logger.Debugf("debug api: peer connect %s: %v", addr, err)
		s.logger.Errorf("unable to connect to peer %s", addr)
		jsonhttp.InternalServerError(w, err)
		return
	}

	if err := s.topologyDriver.Connected(r.Context(), p2p.Peer{Address: bzzAddr.Overlay}, true); err != nil {
		_ = s.p2p.Disconnect(bzzAddr.Overlay, "failed to notify topology")
		s.logger.Debugf("debug api: peer connect handler %s: %v", addr, err)
		s.logger.Errorf("unable to connect to peer %s", addr)
		jsonhttp.InternalServerError(w, err)
		return
	}

	jsonhttp.OK(w, peerConnectResponse{
		Address: bzzAddr.Overlay.String(),
	})
}

func (s *Service) peerDisconnectHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	auroraAddr, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("debug api: parse peer address %s: %v", addr, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}

	if err := s.topologyDriver.DisconnectForce(auroraAddr, "user requested disconnect"); err != nil {
		s.logger.Debugf("debug api: peer disconnect %s: %v", addr, err)
		if errors.Is(err, p2p.ErrPeerNotFound) {
			jsonhttp.BadRequest(w, "peer not found")
			return
		}
		s.logger.Errorf("unable to disconnect peer %s", addr)
		jsonhttp.InternalServerError(w, err)
		return
	}

	jsonhttp.OK(w, nil)
}

func (s *Service) peerBlockingHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	reason := r.URL.Query().Get("reason")
	timeout := r.URL.Query().Get("timeout")

	swarmAddr, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("debug api: parse peer address %s: %v", addr, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		s.logger.Debugf("debug api: parse block timeout %s: %v", timeout, err)
		jsonhttp.BadRequest(w, "invalid block timeout")
		return
	}

	if reason == "" {
		reason = "unknown reason"
	}

	if err := s.p2p.Blocklist(swarmAddr, duration, reason); err != nil {
		s.logger.Debugf("debug api: peer blocking %s: %v", addr, err)
		if errors.Is(err, p2p.ErrPeerNotFound) {
			jsonhttp.BadRequest(w, "peer not found")
			return
		}
		s.logger.Errorf("unable to block peer %s", addr)
		jsonhttp.InternalServerError(w, err)
		return
	}

	jsonhttp.OK(w, nil)
}

type peersResponse struct {
	Peers []p2p.Peer `json:"peers"`
}

func (s *Service) peersHandler(w http.ResponseWriter, r *http.Request) {
	jsonhttp.OK(w, peersResponse{
		Peers: s.p2p.Peers(),
	})
}

func (s *Service) blocklistedPeersHandler(w http.ResponseWriter, r *http.Request) {
	peers, err := s.p2p.BlocklistedPeers()
	if err != nil {
		s.logger.Debugf("debug api: blocklisted peers: %v", err)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	jsonhttp.OK(w, peersResponse{
		Peers: peers,
	})
}
