package api

import (
	"io/ioutil"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gorilla/mux"
)

func (s *server) groupJoinHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}

	err = s.multicast.JoinGroup(r.Context(), gid, nil, model.GroupOption{})
	if err != nil {
		s.logger.Errorf("multicast join group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) groupLeaveHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.multicast.LeaveGroup(gid)
	if err != nil {
		s.logger.Errorf("multicast join group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) multicastMsg(w http.ResponseWriter, r *http.Request) {
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
	err = s.multicast.Multicast(&pb.MulticastMsg{
		Gid:  gid.Bytes(),
		Data: body,
	})
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) groupObserveHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}

	err = s.multicast.ObserveGroup(gid, model.GroupOption{})
	if err != nil {
		s.logger.Errorf("multicast observe group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) groupObserveCancelHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.multicast.ObserveGroupCancel(gid)
	if err != nil {
		s.logger.Errorf("multicast cancel observe group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}
