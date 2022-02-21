package debugapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
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
