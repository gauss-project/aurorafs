package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gorilla/mux"
)

func (s *server) addGroup(w http.ResponseWriter, r *http.Request, gType model.GType) {
	gid := mux.Vars(r)["gid"]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	var req model.ConfigNodeGroup
	if len(body) == 0 {
		jsonhttp.BadRequest(w, fmt.Errorf("missing parameters"))
		return
	}
	err = json.Unmarshal(body, &req)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	if req.KeepPingPeers < 1 && req.KeepConnectedPeers < 1 {
		jsonhttp.BadRequest(w, fmt.Errorf("keep_ping_peers or keep_connected_peers must > 0"))
		return
	}
	req.Name = gid
	req.GType = gType
	err = s.multicast.AddGroup([]model.ConfigNodeGroup{req})
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) groupJoinHandler(w http.ResponseWriter, r *http.Request) {
	s.addGroup(w, r, model.GTypeJoin)
}

func (s *server) groupLeaveHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.multicast.RemoveGroup(gid, model.GTypeJoin)
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
	s.addGroup(w, r, model.GTypeObserve)
}

func (s *server) groupObserveCancelHandler(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	err = s.multicast.RemoveGroup(gid, model.GTypeObserve)
	if err != nil {
		s.logger.Errorf("multicast cancel observe group: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) sendReceive(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	dest := mux.Vars(r)["target"]
	target, err := boson.ParseHexAddress(dest)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	if len(body) == 0 {
		jsonhttp.BadRequest(w, fmt.Errorf("missing body"))
		return
	}
	out, err := s.multicast.SendReceive(r.Context(), body, gid, target)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, struct {
		Data []byte `json:"data"`
	}{Data: out})
}

func (s *server) notify(w http.ResponseWriter, r *http.Request) {
	str := mux.Vars(r)["gid"]
	gid, err := boson.ParseHexAddress(str)
	if err != nil {
		gid = multicast.GenerateGID(str)
	}
	dest := mux.Vars(r)["target"]
	target, err := boson.ParseHexAddress(dest)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	if len(body) == 0 {
		jsonhttp.BadRequest(w, fmt.Errorf("missing body"))
		return
	}
	err = s.multicast.Send(r.Context(), body, gid, target)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}

func (s *server) peers(w http.ResponseWriter, r *http.Request) {
	groupName := mux.Vars(r)["gid"]
	peers, err := s.multicast.GetGroupPeers(groupName)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, peers)
}
