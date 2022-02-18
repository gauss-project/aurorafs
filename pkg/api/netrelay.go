package api

import (
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"io/ioutil"
	"net/http"
	"strings"
)

func (s *server) relayDo(w http.ResponseWriter, r *http.Request) {
	url := r.URL.String()
	url = strings.ReplaceAll(url, aurora.RelayPrefixHttp, "")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {

	}
}
