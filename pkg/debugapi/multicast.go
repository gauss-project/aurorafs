package debugapi

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gorilla/mux"
)

func (s *Service) groupsTopologyHandler(w http.ResponseWriter, r *http.Request) {
	params := s.group.Snapshot()
	b, err := json.Marshal(params)
	if err != nil {
		s.logger.Errorf("multicast marshal to json: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	w.Header().Set("Content-Type", jsonhttp.DefaultContentTypeHeader)
	_, _ = io.Copy(w, bytes.NewBuffer(b))
}

func (s *Service) groupJoinHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}

	err = s.group.JoinGroup(r.Context(), gid, nil)
	if err != nil {
		s.logger.Errorf("multicast join group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *Service) groupLeaveHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.group.LeaveGroup(gid)
	if err != nil {
		s.logger.Errorf("multicast join group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *Service) multicastMsg(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	err = s.group.Multicast(&pb.MulticastMsg{
		Gid:  gid.Bytes(),
		Data: body,
	})
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *Service) groupObserveHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}

	err = s.group.ObserveGroup(gid)
	if err != nil {
		s.logger.Errorf("multicast observe group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *Service) groupObserveCancelHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.group.ObserveGroupCancel(gid)
	if err != nil {
		s.logger.Errorf("multicast cancel observe group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}
