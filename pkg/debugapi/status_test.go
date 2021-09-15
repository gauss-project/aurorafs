package debugapi_test

import (
	"net/http"
	"testing"

	"github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/pkg/debugapi"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
)

func TestHealth(t *testing.T) {
	testServer := newTestServer(t, testServerOptions{})

	jsonhttptest.Request(t, testServer.Client, http.MethodGet, "/health", http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(debugapi.StatusResponse{
			Status:  "ok",
			Version: bee.Version,
		}),
	)
}

func TestReadiness(t *testing.T) {
	testServer := newTestServer(t, testServerOptions{})

	jsonhttptest.Request(t, testServer.Client, http.MethodGet, "/readiness", http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(debugapi.StatusResponse{
			Status:  "ok",
			Version: bee.Version,
		}),
	)
}
