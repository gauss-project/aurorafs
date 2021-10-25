package api_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	testingc "github.com/gauss-project/aurorafs/pkg/storage/testing"
)

func TestGatewayMode(t *testing.T) {
	logger := logging.New(io.Discard, 0)
	chunk := testingc.GenerateTestRandomChunk()
	client, _, _ := newTestServer(t, testServerOptions{
		Storer:      mock.NewStorer(),
		Logger:      logger,
		GatewayMode: true,
	})

	forbiddenResponseOption := jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
		Message: http.StatusText(http.StatusForbidden),
		Code:    http.StatusForbidden,
	})

	t.Run("pinning endpoints", func(t *testing.T) {
		path := "/pins/0773a91efd6547c754fc1d95fb1c62c7d1b47f959c2caa685dfec8736da95c1c"
		jsonhttptest.Request(t, client, http.MethodGet, path, http.StatusForbidden, forbiddenResponseOption)
		jsonhttptest.Request(t, client, http.MethodPost, path, http.StatusForbidden, forbiddenResponseOption)
		jsonhttptest.Request(t, client, http.MethodDelete, path, http.StatusForbidden, forbiddenResponseOption)
		jsonhttptest.Request(t, client, http.MethodGet, "/pins", http.StatusForbidden, forbiddenResponseOption)
	})

	t.Run("pinning", func(t *testing.T) {
		headerOption := jsonhttptest.WithRequestHeader(api.AuroraPinHeader, "true")

		forbiddenResponseOption := jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: "pinning is disabled",
			Code:    http.StatusForbidden,
		})

		// should work without pinning
		jsonhttptest.Request(t, client, http.MethodPost, "/chunks", http.StatusCreated,
			jsonhttptest.WithRequestBody(bytes.NewReader(chunk.Data())),
		)

		jsonhttptest.Request(t, client, http.MethodPost, "/chunks/0773a91efd6547c754fc1d95fb1c62c7d1b47f959c2caa685dfec8736da95c1c", http.StatusForbidden, forbiddenResponseOption, headerOption)

		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusCreated,
			jsonhttptest.WithRequestBody(bytes.NewReader(chunk.Data())),
		) // should work without pinning
		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/files", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/dirs", http.StatusForbidden, forbiddenResponseOption, headerOption)
	})

	t.Run("encryption", func(t *testing.T) {
		headerOption := jsonhttptest.WithRequestHeader(api.AuroraEncryptHeader, "true")

		forbiddenResponseOption := jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: "encryption is disabled",
			Code:    http.StatusForbidden,
		})

		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusCreated,
			jsonhttptest.WithRequestBody(bytes.NewReader(chunk.Data())),
		) // should work without pinning
		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/files", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/dirs", http.StatusForbidden, forbiddenResponseOption, headerOption)
	})
}
