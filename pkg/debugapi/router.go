package debugapi

import (
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"resenje.org/web"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging/httpaccess"
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

	router.Handle("/addresses", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.addressesHandler),
	})

	return router
}

// newRouter construct the complete set of routes after all of the dependencies
// are injected and exposes /readiness endpoint to provide information that
// Debug API is fully active.
func (s *Service) newRouter() *mux.Router {
	router := s.newBasicRouter()

	router.Handle("/readiness", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.statusHandler),
	})

	router.Handle("/pingpong/{peer-id}", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.pingpongHandler),
	})

	router.Handle("/connect/{multi-address:.+}", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.peerConnectHandler),
	})
	router.Handle("/peers", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.peersHandler),
	})
	router.Handle("/blocklist", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.blocklistedPeersHandler),
	})
	router.Handle("/peers/{address}", jsonhttp.MethodHandler{
		"DELETE": http.HandlerFunc(s.peerDisconnectHandler),
	})
	router.Handle("/blocklist/{address}", jsonhttp.MethodHandler{
		"POST": http.HandlerFunc(s.peerBlockingHandler),
	})
	router.Handle("/chunks/{address}", jsonhttp.MethodHandler{
		"GET":    http.HandlerFunc(s.hasChunkHandler),
		"DELETE": http.HandlerFunc(s.removeChunk),
	})
	router.Handle("/topology", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.topologyHandler),
	})
	router.Handle("/welcome-message", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.getWelcomeMessageHandler),
		"POST": web.ChainHandlers(
			jsonhttp.NewMaxBodyBytesHandler(welcomeMessageMaxRequestSize),
			web.FinalHandlerFunc(s.setWelcomeMessageHandler),
		),
	})
	router.Handle("/chunk/discover/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoDiscoverHandler),
	})
	router.Handle("/chunk/server/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoServerHandler),
	})

	router.Handle("/chunk/init/{rootCid}", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.chunkInfoInitHandler),
	})

	router.Handle("/health", jsonhttp.MethodHandler{
		"GET": http.HandlerFunc(s.statusHandler),
	})

	//router.Handle("/balances", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.compensatedBalancesHandler),
	//})
	//
	//router.Handle("/balances/{peer}", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.compensatedPeerBalanceHandler),
	//})
	//
	//router.Handle("/consumed", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.balancesHandler),
	//})
	//
	//router.Handle("/consumed/{peer}", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.peerBalanceHandler),
	//})

	//router.Handle("/settlements", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.settlementsHandler),
	//})
	//
	//router.Handle("/settlements/{peer}", jsonhttp.MethodHandler{
	//	"GET": http.HandlerFunc(s.peerSettlementsHandler),
	//})

	return router
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
