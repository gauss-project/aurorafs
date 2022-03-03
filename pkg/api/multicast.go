package api

import (
	"io/ioutil"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gogf/gf/encoding/gjson"
	"github.com/gorilla/mux"
)

func (s *server) groupJoinHandler(w http.ResponseWriter, r *http.Request) {
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
	j, err := gjson.DecodeToJson(body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	list := j.GetArray("peers")
	var peers []boson.Address
	for _, v := range list {
		addr, err := boson.ParseHexAddress(v.(string))
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		peers = append(peers, addr)
	}
	err = s.multicast.JoinGroup(r.Context(), gid, nil, model.GroupOption{}, peers...)
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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	j, err := gjson.DecodeToJson(body)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	list := j.GetArray("peers")
	var peers []boson.Address
	for _, v := range list {
		addr, err := boson.ParseHexAddress(v.(string))
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		peers = append(peers, addr)
	}
	err = s.multicast.ObserveGroup(gid, model.GroupOption{}, peers...)
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
	out, err := s.multicast.SendReceive(r.Context(), body, gid, target)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, out)
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
	err = s.multicast.Send(r.Context(), body, gid, target)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, nil)
}
