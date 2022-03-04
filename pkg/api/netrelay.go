package api

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"net/http"
	"strings"
)

func (s *server) relayDo(w http.ResponseWriter, r *http.Request) {
	url := strings.ReplaceAll(r.URL.String(), aurora.RelayPrefixHttp, "")
	urls := strings.Split(url, "/")
	group := urls[1]
	node, err := s.multicast.GetMulticastNode(group)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	if boson.ZeroAddress.Equal(node) {
		jsonhttp.InternalServerError(w, fmt.Sprintf("No corresponding node found of group:%s", group))
		return
	}
	s.netRelay.RelayHttpDo(w, r, node)
}