package debugapi

import (
	"net/http"

	"github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

type statusResponse struct {
	Status       string `json:"status"`
	Version      string `json:"version"`
	FullNode     bool   `json:"fullNode"`
	BootNodeMode bool   `json:"bootNodeMode"`
}

func (s *Service) statusHandler(w http.ResponseWriter, r *http.Request) {
	jsonhttp.OK(w, statusResponse{
		Status:       "ok",
		Version:      aufs.Version,
		FullNode:     s.nodeOptions.FullNode,
		BootNodeMode: s.nodeOptions.BootNodeMode,
	})
}
