package debugapi

import (
	"net/http"

	"github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

type statusResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	jsonhttp.OK(w, statusResponse{
		Status:  "ok",
		Version: aufs.Version,
	})
}
