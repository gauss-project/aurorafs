package debugapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

func (s *Service) topologyHandler(w http.ResponseWriter, r *http.Request) {
	params := s.topologyDriver.Snapshot()

	params.LightNodes = s.lightNodes.PeerInfo()
	params.BootNodes = s.bootNodes.PeerInfo()

	b, err := json.Marshal(params)
	if err != nil {
		s.logger.Errorf("topology marshal to json: %v", err)
		jsonhttp.InternalServerError(w, err)
		return
	}
	w.Header().Set("Content-Type", jsonhttp.DefaultContentTypeHeader)
	_, _ = io.Copy(w, bytes.NewBuffer(b))
}
