package debugapi

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gorilla/mux"
	"net/http"
)

func (s *Service) chunkinfoHandler(w http.ResponseWriter, r *http.Request) {
 	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
 	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
 	v := s.chunkInfo.GetChunkInfoOverlays(rootCid)
	jsonhttp.OK(w, v)
}
