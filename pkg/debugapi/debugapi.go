// Package debugapi exposes the debug API used to
// control and analyze low-level and runtime
// features and functionalities of Aurora.
package debugapi

import (
	"crypto/ecdsa"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/pingpong"
	"github.com/gauss-project/aurorafs/pkg/storage"

	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"
)

// Service implements http.Handler interface to be used in HTTP server.
type Service struct {
	overlay         boson.Address
	publicKey       ecdsa.PublicKey
	pssPublicKey    ecdsa.PublicKey
	ethereumAddress common.Address
	p2p             p2p.DebugService
	pingpong        pingpong.Interface
	topologyDriver  topology.Driver
	storer          storage.Storer
	logger          logging.Logger
	tracer          *tracing.Tracer
	lightNodes      *lightnode.Container
	bootNodes       *bootnode.Container
	routetab        routetab.RouteTab
	chunkInfo       chunkinfo.Interface
	retrieval       retrieval.Interface
	//accounting         accounting.Interface
	//settlement         settlement.Interface
	//chequebookEnabled  bool
	//chequebook         chequebook.Service
	//swap               swap.ApiInterface
	corsAllowedOrigins []string
	metricsRegistry    *prometheus.Registry
	// handler is changed in the Configure method
	handler     http.Handler
	handlerMu   sync.RWMutex
	nodeOptions Options
}

type Options struct {
	PrivateKey     *ecdsa.PrivateKey
	NATAddr        string
	EnableWS       bool
	EnableQUIC     bool
	NodeMode       aurora.Model
	LightNodeLimit int
	WelcomeMessage string
	Transaction    []byte
}

// New creates a new Debug API Service with only basic routers enabled in order
// to expose /addresses, /health endpoints, Go metrics and pprof. It is useful to expose
// these endpoints before all dependencies are configured and injected to have
// access to basic debugging tools and /health endpoint.
func New(overlay boson.Address, publicKey, pssPublicKey ecdsa.PublicKey, ethereumAddress common.Address, logger logging.Logger, tracer *tracing.Tracer, corsAllowedOrigins []string, o Options) *Service {
	s := new(Service)
	s.overlay = overlay
	s.publicKey = publicKey
	s.pssPublicKey = pssPublicKey
	s.ethereumAddress = ethereumAddress
	s.logger = logger
	s.tracer = tracer
	s.corsAllowedOrigins = corsAllowedOrigins
	s.metricsRegistry = newMetricsRegistry()
	s.nodeOptions = o
	s.setRouter(s.newBasicRouter())

	return s
}

// Configure injects required dependencies and configuration parameters and
// constructs HTTP routes that depend on them. It is intended and safe to call
// this method only once.
func (s *Service) Configure(p2p p2p.DebugService, pingpong pingpong.Interface, topologyDriver topology.Driver, lightNodes *lightnode.Container, bootNodes *bootnode.Container, storer storage.Storer, route routetab.RouteTab, chunkinfo chunkinfo.Interface, retrieval retrieval.Interface) {
	s.p2p = p2p
	s.pingpong = pingpong
	s.topologyDriver = topologyDriver
	s.storer = storer
	s.lightNodes = lightNodes
	s.bootNodes = bootNodes
	s.routetab = route
	s.chunkInfo = chunkinfo
	s.retrieval = retrieval
	//s.accounting = accounting
	//s.settlement = settlement
	//s.chequebookEnabled = chequebookEnabled
	//s.chequebook = chequebook
	//s.swap = swap

	s.setRouter(s.newRouter())
}

// ServeHTTP implements http.Handler interface.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// protect handler as it is changed by the Configure method
	s.handlerMu.RLock()
	h := s.handler
	s.handlerMu.RUnlock()

	h.ServeHTTP(w, r)
}
