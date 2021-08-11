// Copyright 2020 The Boson Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"resenje.org/web"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging/httpaccess"
)

func (s *server) setupRouting() {
	apiVersion := "v1" // only one api version exists, this should be configurable with more

	handle := func(router *mux.Router, path string, handler http.Handler) {
		router.Handle(path, handler)
		router.Handle("/"+apiVersion+path, handler)
	}

	router := mux.NewRouter()
	router.NotFoundHandler = http.HandlerFunc(jsonhttp.NotFoundHandler)

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Ethereum Boson Bee")
	})

	router.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "User-agent: *\nDisallow: /")
	})

	handle(router, "/files", jsonhttp.MethodHandler{
		"POST": web.ChainHandlers(
			s.newTracingHandler("files-upload"),
			web.FinalHandlerFunc(s.fileUploadHandler),
		),
	})
	handle(router, "/files/{addr}", jsonhttp.MethodHandler{
		"GET": web.ChainHandlers(
			s.newTracingHandler("files-download"),
			web.FinalHandlerFunc(s.fileDownloadHandler),
		),
	})

	handle(router, "/dirs", jsonhttp.MethodHandler{
		"POST": web.ChainHandlers(
			s.newTracingHandler("dirs-upload"),
			web.FinalHandlerFunc(s.dirUploadHandler),
		),
	})

	handle(router, "/bytes", jsonhttp.MethodHandler{
		"POST": web.ChainHandlers(
			s.newTracingHandler("bytes-upload"),
			web.FinalHandlerFunc(s.bytesUploadHandler),
		),
	})
	handle(router, "/bytes/{address}", jsonhttp.MethodHandler{
		"GET": web.ChainHandlers(
			s.newTracingHandler("bytes-download"),
			web.FinalHandlerFunc(s.bytesGetHandler),
		),
	})

	handle(router, "/chunks", jsonhttp.MethodHandler{
		"POST": web.ChainHandlers(
			jsonhttp.NewMaxBodyBytesHandler(boson.ChunkWithSpanSize),
			web.FinalHandlerFunc(s.chunkUploadHandler),
		),
	})

	handle(router, "/chunks/{addr}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkGetHandler),
	})

	handle(router, "/soc/{owner}/{id}", jsonhttp.MethodHandler{
		"POST": web.ChainHandlers(
			jsonhttp.NewMaxBodyBytesHandler(boson.ChunkWithSpanSize),
			web.FinalHandlerFunc(s.socUploadHandler),
		),
	})


	handle(router, "/aurora/{address}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := r.URL
		u.Path += "/"
		http.Redirect(w, r, u.String(), http.StatusPermanentRedirect)
	}))
	handle(router, "/aurora/{address}/{path:.*}", jsonhttp.MethodHandler{
		"GET": web.ChainHandlers(
			s.newTracingHandler("aurora-download"),
			web.FinalHandlerFunc(s.bzzDownloadHandler),
		),
	})


	s.Handler = web.ChainHandlers(
		httpaccess.NewHTTPAccessLogHandler(s.logger, logrus.InfoLevel, s.tracer, "api access"),
		handlers.CompressHandler,
		// todo: add recovery handler
		s.pageviewMetricsHandler,
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if o := r.Header.Get("Origin"); o != "" && s.checkOrigin(r) {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
					w.Header().Set("Access-Control-Allow-Origin", o)
					w.Header().Set("Access-Control-Allow-Headers", "Origin, Accept, Authorization, Content-Type, X-Requested-With, Access-Control-Request-Headers, Access-Control-Request-Method")
					w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS, POST, PUT, DELETE")
					w.Header().Set("Access-Control-Max-Age", "3600")
				}
				h.ServeHTTP(w, r)
			})
		},
		s.gatewayModeForbidHeadersHandler,
		web.FinalHandler(router),
	)
}

func (s *server) gatewayModeForbidEndpointHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.GatewayMode {
			s.logger.Tracef("gateway mode: forbidden %s", r.URL.String())
			jsonhttp.Forbidden(w, nil)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *server) gatewayModeForbidHeadersHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.GatewayMode {
			if strings.ToLower(r.Header.Get(BosonPinHeader)) == "true" {
				s.logger.Tracef("gateway mode: forbidden pinning %s", r.URL.String())
				jsonhttp.Forbidden(w, "pinning is disabled")
				return
			}
			if strings.ToLower(r.Header.Get(BosonEncryptHeader)) == "true" {
				s.logger.Tracef("gateway mode: forbidden encryption %s", r.URL.String())
				jsonhttp.Forbidden(w, "encryption is disabled")
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}
