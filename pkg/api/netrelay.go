package api

import (
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

func (s *server) relayDo(w http.ResponseWriter, r *http.Request) {
	s.netRelay.RelayHttpDo(w, r, boson.ZeroAddress)
}
