package netrelay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/netrelay/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab"
)

type NetRelay interface {
	RelayHttpDo(w http.ResponseWriter, r *http.Request, address boson.Address)
}
type Service struct {
	streamer  p2p.Streamer
	logger    logging.Logger
	route     routetab.RouteTab
	groups    []model.ConfigNodeGroup
	multicast multicast.GroupInterface
}

func New(streamer p2p.Streamer, logging logging.Logger, groups []model.ConfigNodeGroup, route routetab.RouteTab, multicast multicast.GroupInterface) *Service {
	return &Service{streamer: streamer, logger: logging, groups: groups, route: route, multicast: multicast}
}

func (s *Service) RelayHttpDo(w http.ResponseWriter, r *http.Request, address boson.Address) {
	url := strings.ReplaceAll(r.URL.String(), aurora.RelayPrefixHttp, "")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, fmt.Errorf("error in getting body:%v", err.Error()))
		return
	}

	mpHeader := make(map[string]string)
	method := r.Method
	for k, v := range r.Header {
		mpHeader[k] = v[0]
	}
	header, err := json.Marshal(mpHeader)
	if err != nil {
		jsonhttp.InternalServerError(w, fmt.Errorf("error in getting header:%v", err.Error()))
		return
	}

	var resp pb.RelayHttpResp
	var msg pb.RelayHttpReq
	msg.Url = url
	msg.Method = []byte(method)
	msg.Body = body
	msg.Header = header

	if boson.ZeroAddress.Equal(address) {
		urls := strings.Split(url, "/")
		group := urls[1]
		nodes, err1 := s.multicast.GetGroupPeers(group)
		if err1 != nil {
			jsonhttp.InternalServerError(w, err1)
			return
		}

		if len(nodes.Connected) == 0 && len(nodes.Keep) == 0 {
			jsonhttp.InternalServerError(w, fmt.Sprintf("No corresponding node found of group:%s", group))
			return
		}
		nodes.Connected = append(nodes.Connected, nodes.Keep...)
		for _, v := range nodes.Connected {
			resp, err = s.SendHttp(r.Context(), v, msg)
			if err == nil {
				break
			}
		}
	} else {
		resp, err = s.SendHttp(r.Context(), address, msg)
	}
	if err != nil {
		jsonhttp.InternalServerError(w, fmt.Errorf("send http %s", err))
		return
	}

	if len(resp.Header) > 0 {
		resMpHeader := make(map[string]string)
		err = json.Unmarshal(resp.Header, &resMpHeader)
		if err != nil {
			jsonhttp.InternalServerError(w, fmt.Errorf("error in returning header parsing:%v", err.Error()))
			return
		}
		for k, v := range resMpHeader {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(int(resp.Status))
	_, _ = io.Copy(w, bytes.NewBuffer(resp.Body))
}

func (s *Service) getDomainAddr(groupName, domainName string) (string, bool) {
	for _, v := range s.groups {
		if v.Name == groupName {
			for _, domain := range v.AgentHttp {
				if domain.Domain == domainName {
					return domain.Addr, true
				}
			}
		}
	}
	s.logger.Errorf("domain %v not found in group %s", domainName, groupName)
	return "", false
}
