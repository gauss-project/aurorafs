package api

import (
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"net/http"
	"strings"
)

func (s *server) relayDo(w http.ResponseWriter, r *http.Request) {
	url := strings.ReplaceAll(r.URL.String(), aurora.RelayPrefixHttp, "")
	urls := strings.Split(url, "/")
	group := urls[1]
	node, directNode, err := s.multicast.GetMulticastNode(group)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	s.netRelay.RelayHttpDo(w, r, node, directNode)
}
