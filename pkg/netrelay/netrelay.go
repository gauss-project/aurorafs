package netrelay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/netrelay/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/routetab"
)

type NetRelay interface {
	RelayHttpDo(w http.ResponseWriter, r *http.Request, address boson.Address)
}
type Service struct {
	streamer p2p.Streamer
	logger   logging.Logger
	address  common.Address
	route    routetab.RouteTab
	groups   []model.ConfigNodeGroup
}

func New(streamer p2p.Streamer, logging logging.Logger, groups []model.ConfigNodeGroup, route routetab.RouteTab) *Service {
	return &Service{streamer: streamer, logger: logging, groups: groups, route: route}
}

func (s *Service) RelayHttpDo(w http.ResponseWriter, r *http.Request, address boson.Address) {
	mpHeader := make(map[string]string)
	url := r.URL.String()
	url = strings.ReplaceAll(url, aurora.RelayPrefixHttp, "")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonhttp.InternalServerError(w, fmt.Errorf("error in getting body:%v", err.Error()))
		return
	}

	method := r.Method
	for k, v := range r.Header {
		mpHeader[k] = v[0]
	}
	header, err := json.Marshal(mpHeader)
	if err != nil {
		jsonhttp.InternalServerError(w, fmt.Errorf("error in getting header:%v", err.Error()))
		return
	}

	var msg pb.RelayHttpReq
	msg.Url = url
	msg.Method = []byte(method)
	msg.Body = body
	msg.Header = header
	resp, err := s.SendHttp(context.Background(), address, msg)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
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
	io.Copy(w, bytes.NewBuffer(resp.Body))

}

func (s *Service) getDomainAddr(goupName, domainName string) (string, bool) {
	for _, v := range s.groups {
		if v.Name == goupName {
			for _, domain := range v.AgentHttp {
				if domain.Domain == domainName {
					return domain.Addr, true
				}
			}
		}
	}
	s.logger.Errorf("netrelay: domain %v address not found ", domainName)
	return "", false
}
