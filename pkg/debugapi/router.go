package debugapi

import (
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/gauss-project/aurorafs/pkg/auth"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging/httpaccess"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"resenje.org/web"
)

// newBasicRouter constructs only the routes that do not depend on the injected dependencies:
// - /health
// - pprof
// - vars
// - metrics
// - /addresses
func (s *Service) newBasicRouter() *mux.Router {
	router := mux.NewRouter()
	router.NotFoundHandler = http.HandlerFunc(jsonhttp.NotFoundHandler)

	router.Path("/metrics").Handler(web.ChainHandlers(
		httpaccess.SetAccessLogLevelHandler(0), // suppress access log messages
		web.FinalHandler(promhttp.InstrumentMetricHandler(
			s.metricsRegistry,
			promhttp.HandlerFor(s.metricsRegistry, promhttp.HandlerOpts{}),
		)),
	))

	router.Handle("/debug/pprof", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := r.URL
		u.Path += "/"
		http.Redirect(w, r, u.String(), http.StatusPermanentRedirect)
	}))
	router.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	router.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	router.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	router.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	router.PathPrefix("/debug/pprof/").Handler(http.HandlerFunc(pprof.Index))

	router.Handle("/debug/vars", expvar.Handler())

	router.Handle("/health", web.ChainHandlers(
		httpaccess.SetAccessLogLevelHandler(0), // suppress access log messages
		web.FinalHandlerFunc(s.statusHandler),
	))

	var handle = func(path string, handler http.Handler) {
		if s.restricted {
			handler = web.ChainHandlers(auth.PermissionCheckHandler(s.auth), web.FinalHandler(handler))
		}
		router.Handle(path, handler)
	}

	handle("/addresses", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.addressesHandler),
	})

	return router
}

// newRouter construct the complete set of routes after all of the dependencies
// are injected and exposes /readiness endpoint to provide information that
// Debug API is fully active.
func (s *Service) newRouter() *mux.Router {
	router := s.newBasicRouter()

	router.Handle("/readiness", web.ChainHandlers(
		httpaccess.SetAccessLogLevelHandler(0), // suppress access log messages
		web.FinalHandlerFunc(s.statusHandler),
	))

	var handle = func(path string, handler http.Handler) {
		if s.restricted {
			handler = web.ChainHandlers(auth.PermissionCheckHandler(s.auth), web.FinalHandler(handler))
		}
		router.Handle(path, handler)
	}

	handle("/pingpong/{peer-id}", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.pingpongHandler),
	})
	handle("/connect/{multi-address:.+}", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.peerConnectHandler),
	})
	handle("/peers", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.peersHandler),
	})
	handle("/peers/{address}", jsonhttp.MethodHandler{
		"DELETE": http.HandlerFunc(s.peerDisconnectHandler),
	})
	handle("/blocklist", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.blocklistedPeersHandler),
	})
	handle("/blocklist/{address}", jsonhttp.MethodHandler{
		"POST":   http.HandlerFunc(s.peerBlockingHandler),
		"DELETE": http.HandlerFunc(s.peerRemoveBlockingHandler),
	})
	handle("/chunks/{address}", jsonhttp.MethodHandler{
		"GET":    http.HandlerFunc(s.hasChunkHandler),
		"DELETE": http.HandlerFunc(s.removeChunk),
	})
	handle("/topology", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.topologyHandler),
	})
	if s.group != nil {
		handle("/topology/group", jsonhttp.MethodHandler{
			"GET": http.HandlerFunc(s.groupsTopologyHandler),
		})
	}
	handle("/route/{peer-id}", jsonhttp.MethodHandler{
		"GET":    http.HandlerFunc(s.getRouteHandel),
		"POST":   http.HandlerFunc(s.findRouteHandel),
		"DELETE": http.HandlerFunc(s.delRouteHandel),
	})
	handle("/route/findunderlay/{peer-id}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.findUnderlayHandel),
	})
	handle("/welcome-message", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.getWelcomeMessageHandler),
		"POST": web.ChainHandlers(
			jsonhttp.NewMaxBodyBytesHandler(welcomeMessageMaxRequestSize),
			web.FinalHandlerFunc(s.setWelcomeMessageHandler),
		),
	})
	handle("/chunk/discover/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoDiscoverHandler),
	})
	handle("/chunk/server/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoServerHandler),
	})
	handle("/chunk/init/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoInitHandler),
	})
	handle("/chunk/source/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoSource),
	})
	handle("/aco/{timestamp}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.getRouteScoreHandle),
	})
	handle("/traffic/init", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.trafficInit),
	})

	s.newLoopbackRouter(router)

	return router
}

func (s *Service) newLoopbackRouter(router *mux.Router) {
	var handle = func(path string, handler http.Handler) {
		handler = web.ChainHandlers(
			// auth.AllowLoopbackIP(),
			web.FinalHandler(handler),
		)
		router.Handle(path, handler)
	}
	handle("/keystore", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.exportKeyHandler),
		"PUT":  http.HandlerFunc(s.importKeyHandler),
	})
}

// setRouter sets the base Debug API handler with common middlewares.
func (s *Service) setRouter(router http.Handler) {
	h := http.NewServeMux()
	h.Handle("/", web.ChainHandlers(
		httpaccess.NewHTTPAccessLogHandler(s.logger, logrus.InfoLevel, s.tracer, "debug api access"),
		handlers.CompressHandler,
		s.corsHandler,
		web.NoCacheHeadersHandler,
		web.FinalHandler(router),
	))

	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()

	s.handler = h
}
