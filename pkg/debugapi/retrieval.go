package debugapi

import (
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

func (s *Service) getRouteScoreHandle(w http.ResponseWriter, r *http.Request) {

	time, err := strconv.ParseInt(mux.Vars(r)["timestamp"], 10, 64)
	if err != nil {
		s.logger.Debugf("debug api: parse aco route score: %v", err)
		jsonhttp.BadRequest(w, "bad timestamp")
		return
	}
	v := s.retrieval.GetRouteScore(time)
	jsonhttp.OK(w, v)
}
