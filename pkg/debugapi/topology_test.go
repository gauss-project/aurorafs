package debugapi_test

import (
	"net/http"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
)

func TestTopologyOK(t *testing.T) {
	testServer := newTestServer(t, testServerOptions{})

	var body []byte
	opts := jsonhttptest.WithPutResponseBody(&body)
	jsonhttptest.Request(t, testServer.Client, http.MethodGet, "/topology", http.StatusOK, opts)

	if len(body) == 0 {
		t.Error("empty response")
	}
}
