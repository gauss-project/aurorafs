// Copyright 2020 The Boson Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api_test

import (
	"bytes"

	"net/http"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"


)

func TestGatewayMode(t *testing.T) {
	client, _, _ := newTestServer(t, testServerOptions{
		Storer:      mock.NewStorer(),
		
		GatewayMode: true,
	})



	t.Run("encryption", func(t *testing.T) {
		headerOption := jsonhttptest.WithRequestHeader(api.BosonEncryptHeader, "true")

		forbiddenResponseOption := jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: "encryption is disabled",
			Code:    http.StatusForbidden,
		})

		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusOK,
			jsonhttptest.WithRequestBody(bytes.NewReader(chunk.Data())),
		) // should work without pinning
		jsonhttptest.Request(t, client, http.MethodPost, "/bytes", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/files", http.StatusForbidden, forbiddenResponseOption, headerOption)
		jsonhttptest.Request(t, client, http.MethodPost, "/dirs", http.StatusForbidden, forbiddenResponseOption, headerOption)
	})
}
