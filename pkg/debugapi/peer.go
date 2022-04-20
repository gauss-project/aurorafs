package debugapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
	"github.com/gorilla/mux"
	"github.com/multiformats/go-multiaddr"
)

type peerConnectResponse struct {
	Address string `json:"address"`
}

func (s *Service) peerConnectHandler(w http.ResponseWriter, r *http.Request) {
	dest := mux.Vars(r)["multi-address"]
	addr, err := multiaddr.NewMultiaddr("/" + dest)
	if err != nil {
		auroraAddr, e := boson.ParseHexAddress(dest)
		if e != nil {
			s.logger.Debugf("debug api: peer connect: parse multiaddress: %v", err)
			jsonhttp.BadRequest(w, err)
			return
		}
		err = s.routetab.Connect(r.Context(), auroraAddr)
		if err != nil {
			s.logger.Debugf("debug api: peer connect %s: %v", auroraAddr, err)
			s.logger.Errorf("unable to connect to peer %s", auroraAddr)
			jsonhttp.InternalServerError(w, err)
			return
		}
		jsonhttp.OK(w, peerConnectResponse{
			Address: auroraAddr.String(),
		})
		return
	}

	peer, err := s.p2p.Connect(r.Context(), addr)
	if err != nil {
		s.logger.Debugf("debug api: peer connect %s: %v", addr, err)
		s.logger.Errorf("unable to connect to peer %s", addr)
		jsonhttp.InternalServerError(w, err)
		return
	}
	s.topologyDriver.Outbound(*peer)

	jsonhttp.OK(w, peerConnectResponse{
		Address: peer.Address.String(),
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

	bosonAddr, err := boson.ParseHexAddress(addr)
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

	if err := s.p2p.Blocklist(bosonAddr, duration, reason); err != nil {
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
	Peers []PeerItem `json:"peers"`
}

type PeerItem struct {
	Address   boson.Address `json:"address"`
	FullNode  bool          `json:"fullNode"`
	Direction string        `json:"direction"`
}

func (s *Service) peersHandler(w http.ResponseWriter, r *http.Request) {
	jsonhttp.OK(w, peersResponse{
		Peers: s.convPeer(s.p2p.Peers()),
	})
}

func (s *Service) convPeer(peers []p2p.Peer) []PeerItem {
	list := make([]PeerItem, 0)
	for _, v := range peers {
		tmp := PeerItem{
			Address: v.Address,
			FullNode: func() bool {
				if v.Mode.Bv != nil {
					return v.Mode.IsFull()
				}
				return false
			}(),
		}
		m := s.topologyDriver.SnapshotAddr(v.Address)
		if m != nil {
			tmp.Direction = string(m.SessionConnectionDirection)
		}
		if !tmp.FullNode {
			tmp.Direction = string(model.PeerConnectionDirectionInbound)
		}
		list = append(list, tmp)
	}
	return list
}

type blockPeersResponse struct {
	Peers []p2p.BlockPeers `json:"peers"`
}

func (s *Service) blocklistedPeersHandler(w http.ResponseWriter, r *http.Request) {
	peers, err := s.p2p.BlocklistedPeers()
	if err != nil {
		s.logger.Debugf("debug api: blocklisted peers: %v", err)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	jsonhttp.OK(w, blockPeersResponse{
		Peers: peers,
	})
}

func (s *Service) peerRemoveBlockingHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]

	bosonAddr, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("debug api: parse peer address %s: %v", addr, err)
		jsonhttp.BadRequest(w, "invalid peer address")
		return
	}

	err = s.p2p.BlocklistRemove(bosonAddr)
	if err != nil {
		s.logger.Debugf("debug api: blocklisted remove peers: %v", err)
		jsonhttp.InternalServerError(w, nil)
		return
	}
	jsonhttp.OK(w, nil)
}
