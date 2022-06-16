package debugapi

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gorilla/mux"
	"net/http"
)

type chunkInfoIntoResp struct {
	Msg bool `json:"msg"`
}

func (s *Service) chunkInfoDiscoverHandler(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.fileInfo.GetChunkInfoDiscoverOverlays(rootCid)
	jsonhttp.OK(w, v)
}
func (s *Service) chunkInfoServerHandler(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.fileInfo.GetChunkInfoServerOverlays(rootCid)
	jsonhttp.OK(w, v)
}
func (s *Service) chunkInfoInitHandler(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.chunkInfo.Discover(r.Context(), nil, rootCid)
	jsonhttp.OK(w, chunkInfoIntoResp{
		Msg: v,
	})
}

func (s *Service) chunkInfoSource(w http.ResponseWriter, r *http.Request) {
	rootCid, err := boson.ParseHexAddress(mux.Vars(r)["rootCid"])
	if err != nil {
		s.logger.Debugf("debug api: parse chunk info rootCid: %v", err)
		jsonhttp.BadRequest(w, "bad rootCid")
		return
	}
	v := s.fileInfo.GetChunkInfoSource(rootCid)
	jsonhttp.OK(w, v)
}
