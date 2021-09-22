package debugapi

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gorilla/mux"
	"net/http"
)

func (s *Service) chunkInfoDiscoverHandler(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.chunkInfo.GetChunkInfoDiscoverOverlays(rootCid)
	jsonhttp.OK(w, v)
}
func (s *Service) chunkInfoServerHandler(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.chunkInfo.GetChunkInfoServerOverlays(rootCid)
	jsonhttp.OK(w, v)
}
