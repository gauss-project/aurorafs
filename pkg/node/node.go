// Package node defines the concept of a Aurora node
// by bootstrapping and injecting all necessary
// dependencies.
package node

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gauss-project/aurorafs/pkg/accounting"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/auth"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/crypto/cert"
	"github.com/gauss-project/aurorafs/pkg/debugapi"
	"github.com/gauss-project/aurorafs/pkg/hive2"
	"github.com/gauss-project/aurorafs/pkg/localstore"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/gauss-project/aurorafs/pkg/multicast"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/netrelay"
	"github.com/gauss-project/aurorafs/pkg/netstore"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/pingpong"
	"github.com/gauss-project/aurorafs/pkg/pinning"
	"github.com/gauss-project/aurorafs/pkg/resolver/multiresolver"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/gogf/gf/v2/util/gconv"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Aurora struct {
	p2pService       io.Closer
	p2pCancel        context.CancelFunc
	apiCloser        io.Closer
	apiServer        *http.Server
	debugAPIServer   *http.Server
	resolverCloser   io.Closer
	errorLogWriter   *io.PipeWriter
	tracerCloser     io.Closer
	groupCloser      io.Closer
	stateStoreCloser io.Closer
	localstoreCloser io.Closer
	topologyCloser   io.Closer
	ethClientCloser  func()
	// recoveryHandleCleanup func()
}

type Options struct {
	DataDir               string
	CacheCapacity         uint64
	DBDriver              string
	DBPath                string
	HTTPAddr              string
	WSAddr                string
	APIAddr               string
	DebugAPIAddr          string
	ApiBufferSizeMul      int
	NATAddr               string
	EnableWS              bool
	EnableQUIC            bool
	WelcomeMessage        string
	Bootnodes             []string
	ChainEndpoint         string
	OracleContractAddress string
	CORSAllowedOrigins    []string
	Logger                logging.Logger
	Standalone            bool
	IsDev                 bool
	TracingEnabled        bool
	TracingEndpoint       string
	TracingServiceName    string
	// GlobalPinningEnabled     bool
	// PaymentThreshold         string
	// PaymentTolerance         string
	// PaymentEarly             string
	ResolverConnectionCfgs []multiresolver.ConnectionConfig
	GatewayMode            bool
	TrafficEnable          bool
	TrafficContractAddr    string
	KadBinMaxPeers         int
	LightNodeMaxPeers      int
	AllowPrivateCIDRs      bool
	Restricted             bool
	TokenEncryptionKey     string
	AdminPasswordHash      string
	RouteAlpha             int32
	Groups                 interface{}
	EnableApiTLS           bool
	TlsCrtFile             string
	TlsKeyFile             string
}

func NewAurora(nodeMode aurora.Model, addr string, bosonAddress boson.Address, publicKey ecdsa.PublicKey, signer crypto.Signer, networkID uint64, logger logging.Logger, libp2pPrivateKey *ecdsa.PrivateKey, o Options) (b *Aurora, err error) {
	tracer, tracerCloser, err := tracing.NewTracer(&tracing.Options{
		Enabled:     o.TracingEnabled,
		Endpoint:    o.TracingEndpoint,
		ServiceName: o.TracingServiceName,
	})
	if err != nil {
		return nil, fmt.Errorf("tracer: %w", err)
	}

	p2pCtx, p2pCancel := context.WithCancel(context.Background())
	defer func() {
		// if there's been an error on this function
		// we'd like to cancel the p2p context so that
		// incoming connections will not be possible
		if err != nil {
			p2pCancel()
		}
	}()

	b = &Aurora{
		p2pCancel:      p2pCancel,
		errorLogWriter: logger.WriterLevel(logrus.ErrorLevel),
		tracerCloser:   tracerCloser,
	}

	// a struct warped publish-subscribe function
	subPub := subscribe.NewSubPub()

	var authenticator *auth.Authenticator

	if o.Restricted {
		if authenticator, err = auth.New(o.TokenEncryptionKey, o.AdminPasswordHash, logger); err != nil {
			return nil, fmt.Errorf("authenticator: %w", err)
		}
		logger.Info("starting with restricted APIs")
	}

	var debugAPIService *debugapi.Service

	if o.EnableApiTLS && o.TlsKeyFile == "" && o.TlsCrtFile == "" {
		// auto create
		o.TlsCrtFile, o.TlsKeyFile = cert.GenerateCert(o.DataDir)
	}

	if o.DebugAPIAddr != "" {
		// set up basic debug api endpoints for debugging and /health endpoint
		debugAPIService = debugapi.New(bosonAddress, publicKey, logger, tracer, o.CORSAllowedOrigins, o.Restricted, authenticator, debugapi.Options{
			DataDir:        o.DataDir,
			NATAddr:        o.NATAddr,
			NetworkID:      networkID,
			EnableWS:       o.EnableWS,
			EnableQUIC:     o.EnableQUIC,
			NodeMode:       nodeMode,
			WelcomeMessage: o.WelcomeMessage,
			LightNodeLimit: o.LightNodeMaxPeers,
		})

		debugAPIListener, err := net.Listen("tcp", o.DebugAPIAddr)
		if err != nil {
			return nil, fmt.Errorf("debug api listener: %w", err)
		}

		debugAPIServer := &http.Server{
			IdleTimeout:       30 * time.Second,
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           debugAPIService,
			ErrorLog:          log.New(b.errorLogWriter, "", 0),
		}

		go func() {
			if o.EnableApiTLS {
				logger.Infof("debug api address: https://%s", debugAPIListener.Addr())
				err = debugAPIServer.ServeTLS(debugAPIListener, o.TlsCrtFile, o.TlsKeyFile)
				if err != nil {
					logger.Errorf("debug api server enable https: %v", err)
				}
			}
			logger.Infof("debug api address: http://%s", debugAPIListener.Addr())
			err = debugAPIServer.Serve(debugAPIListener)
			if err != nil && err != http.ErrServerClosed {
				logger.Debugf("debug api server: %v", err)
				logger.Error("unable to serve debug api")
			}
		}()

		b.debugAPIServer = debugAPIServer
	}

	stateStore, err := InitStateStore(logger, o.DataDir)
	if err != nil {
		return nil, err
	}
	b.stateStoreCloser = stateStore

	err = CheckOverlayWithStore(bosonAddress, stateStore)
	if err != nil {
		return nil, err
	}

	addressBook := addressbook.New(stateStore)
	lightNodes := lightnode.NewContainer(bosonAddress)
	bootNodes := bootnode.NewContainer(bosonAddress)
	p2ps, err := libp2p.New(p2pCtx, signer, networkID, bosonAddress, addr, addressBook, stateStore, lightNodes, bootNodes, logger, tracer, libp2p.Options{
		PrivateKey:     libp2pPrivateKey,
		NATAddr:        o.NATAddr,
		EnableWS:       o.EnableWS,
		EnableQUIC:     o.EnableQUIC,
		WelcomeMessage: o.WelcomeMessage,
		NodeMode:       nodeMode,
		LightNodeLimit: o.LightNodeMaxPeers,
		KadBinMaxPeers: o.KadBinMaxPeers,
	})

	if err != nil {
		return nil, fmt.Errorf("p2p service: %w", err)
	}

	oracleChain, settlement, apiInterface, commonChain, err := InitChain(
		p2pCtx,
		logger,
		o.ChainEndpoint,
		o.OracleContractAddress,
		stateStore,
		signer,
		o.TrafficEnable,
		o.TrafficContractAddr,
		p2ps,
		subPub)
	if err != nil {
		return nil, err
	}
	b.p2pService = p2ps

	if !o.Standalone {
		if natManager := p2ps.NATManager(); natManager != nil {
			// wait for nat manager to init
			logger.Debug("initializing NAT manager")
			select {
			case <-natManager.Ready():
				// this is magic sleep to give NAT time to sync the mappings
				// this is a hack, kind of alchemy and should be improved
				time.Sleep(3 * time.Second)
				logger.Debug("NAT manager initialized")
			case <-time.After(10 * time.Second):
				logger.Warning("NAT manager init timeout")
			}
		}
	}

	// Construct protocols.
	pingPong := pingpong.New(p2ps, logger, tracer)

	if err = p2ps.AddProtocol(pingPong.Protocol()); err != nil {
		return nil, fmt.Errorf("pingpong service: %w", err)
	}

	var bootnodes []ma.Multiaddr
	if o.Standalone {
		logger.Info("Starting node in standalone mode, no p2p connections will be made or accepted")
	} else {
		for _, a := range o.Bootnodes {
			addr, err := ma.NewMultiaddr(a)
			if err != nil {
				logger.Debugf("multiaddress fail %s: %v", a, err)
				logger.Warningf("invalid bootnode address %s", a)
				continue
			}

			bootnodes = append(bootnodes, addr)
		}
	}

	paymentThreshold := new(big.Int).SetUint64(256 * 4 * 4)
	paymentTolerance := new(big.Int).Mul(paymentThreshold, new(big.Int).SetUint64(2*32))

	acc := accounting.NewAccounting(
		paymentTolerance,
		paymentThreshold,
		logger,
		stateStore,
		settlement,
	)
	settlement.SetNotifyPaymentFunc(acc.AsyncNotifyPayment)

	metricsDB, err := shed.NewDBWrap(stateStore.DB())
	if err != nil {
		return nil, fmt.Errorf("unable to create metrics storage for kademlia: %w", err)
	}

	hiveObj := hive2.New(p2ps, addressBook, networkID, logger)
	if err = p2ps.AddProtocol(hiveObj.Protocol()); err != nil {
		return nil, fmt.Errorf("hive service: %w", err)
	}

	kad, err := kademlia.New(bosonAddress, addressBook, hiveObj, p2ps, pingPong, lightNodes, bootNodes, metricsDB, logger, subPub, kademlia.Options{
		Bootnodes:   bootnodes,
		NodeMode:    nodeMode,
		BinMaxPeers: o.KadBinMaxPeers,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create kademlia: %w", err)
	}
	b.topologyCloser = kad
	hiveObj.SetAddPeersHandler(kad.AddPeers)
	hiveObj.SetConfig(hive2.Config{Kad: kad, Base: bosonAddress, AllowPrivateCIDRs: o.AllowPrivateCIDRs}) // hive2

	p2ps.SetPickyNotifier(kad)
	addrs, err := p2ps.Addresses()
	if err != nil {
		return nil, fmt.Errorf("get server addresses: %w", err)
	}

	for _, addr := range addrs {
		logger.Debugf("p2p address: %s", addr)
	}

	route := routetab.New(bosonAddress, p2pCtx, p2ps, p2ps, addressBook, networkID, lightNodes, kad, stateStore, logger, routetab.Options{Alpha: o.RouteAlpha})
	if err = p2ps.AddProtocol(route.Protocol()); err != nil {
		return nil, fmt.Errorf("routetab service: %w", err)
	}

	p2ps.ApplyRoute(bosonAddress, route, nodeMode)

	var path string

	if o.DBPath != "" {
		path = o.DBPath
	} else if o.DataDir != "" {
		path = filepath.Join(o.DataDir, "localstore")
	}
	lo := &localstore.Options{
		Capacity: o.CacheCapacity,
		Driver:   o.DBDriver,
	}
	storer, err := localstore.New(path, bosonAddress.Bytes(), lo, logger)
	if err != nil {
		return nil, fmt.Errorf("localstore: %w", err)
	}
	b.localstoreCloser = storer

	retrieve := retrieval.New(bosonAddress, p2ps, route, storer, nodeMode.IsFull(), logger, tracer, acc, subPub)
	if err = p2ps.AddProtocol(retrieve.Protocol()); err != nil {
		return nil, fmt.Errorf("retrieval service: %w", err)
	}

	ns := netstore.New(storer, retrieve, logger, bosonAddress)

	traversalService := traversal.New(ns)

	pinningService := pinning.NewService(storer, stateStore, traversalService)

	multiResolver := multiresolver.NewMultiResolver(
		multiresolver.WithDefaultEndpoint(o.ChainEndpoint),
		multiresolver.WithConnectionConfigs(o.ResolverConnectionCfgs),
		multiresolver.WithLogger(o.Logger),
	)
	b.resolverCloser = multiResolver

	chunkInfo := chunkinfo.New(bosonAddress, p2ps, logger, traversalService, stateStore, ns, route, oracleChain, multiResolver, subPub)
	if err := chunkInfo.InitChunkInfo(); err != nil {
		return nil, fmt.Errorf("chunk info init: %w", err)
	}
	if err = p2ps.AddProtocol(chunkInfo.Protocol()); err != nil {
		return nil, fmt.Errorf("chunkInfo service: %w", err)
	}
	storer.SetChunkInfo(chunkInfo)
	ns.SetChunkInfo(chunkInfo)
	retrieve.Config(chunkInfo)

	group := multicast.NewService(bosonAddress, nodeMode, p2ps, p2ps, kad, route, logger, subPub, multicast.Option{Dev: o.IsDev})
	group.Start()
	b.groupCloser = group
	err = p2ps.AddProtocol(group.Protocol())
	if err != nil {
		return nil, err
	}
	var configGroups []model.ConfigNodeGroup
	if o.Groups != nil {
		err = gconv.Struct(o.Groups, &configGroups)
		if err != nil {
			logger.Errorf("Group configuration acquisition failed: %v", err)
			return nil, err
		}
		err = group.AddGroup(configGroups)
		if err != nil {
			return nil, err
		}
	}

	relay := netrelay.New(p2ps, logger, configGroups, route, group)
	err = p2ps.AddProtocol(relay.Protocol())
	if err != nil {
		return nil, err
	}

	var apiService api.Service
	if o.APIAddr != "" {
		// API server
		apiService = api.New(ns, multiResolver, bosonAddress, chunkInfo, traversalService, pinningService,
			authenticator, logger, tracer, apiInterface, commonChain, oracleChain, relay, group,
			api.Options{
				CORSAllowedOrigins: o.CORSAllowedOrigins,
				GatewayMode:        o.GatewayMode,
				WsPingPeriod:       60 * time.Second,
				BufferSizeMul:      o.ApiBufferSizeMul,
				Restricted:         o.Restricted,
				DebugApiAddr:       o.DebugAPIAddr,
				RPCWSAddr:          o.WSAddr,
			})
		apiListener, err := net.Listen("tcp", o.APIAddr)
		if err != nil {
			return nil, fmt.Errorf("api listener: %w", err)
		}

		apiServer := &http.Server{
			IdleTimeout:       30 * time.Second,
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           apiService,
			ErrorLog:          log.New(b.errorLogWriter, "", 0),
		}

		go func() {
			if o.EnableApiTLS {
				logger.Infof("api address: https://%s", apiListener.Addr())
				err = apiServer.ServeTLS(apiListener, o.TlsCrtFile, o.TlsKeyFile)
				if err != nil {
					logger.Errorf("api server enable https: %v", err)
				}
			}
			logger.Infof("api address: http://%s", apiListener.Addr())
			err = apiServer.Serve(apiListener)
			if err != nil && err != http.ErrServerClosed {
				logger.Debugf("api server: %v", err)
				logger.Error("unable to serve api")
			}
		}()

		b.apiServer = apiServer
		b.apiCloser = apiService
	}

	if debugAPIService != nil {
		// register metrics from components
		debugAPIService.MustRegisterMetrics(p2ps.Metrics()...)
		debugAPIService.MustRegisterMetrics(pingPong.Metrics()...)
		// debugAPIService.MustRegisterMetrics(acc.Metrics()...)
		debugAPIService.MustRegisterMetrics(storer.Metrics()...)
		debugAPIService.MustRegisterMetrics(kad.Metrics()...)
		debugAPIService.MustRegisterMetrics(lightNodes.Metrics()...)
		debugAPIService.MustRegisterMetrics(bootNodes.Metrics()...)
		debugAPIService.MustRegisterMetrics(hiveObj.Metrics()...)
		debugAPIService.MustRegisterMetrics(chunkInfo.Metrics()...)
		debugAPIService.MustRegisterMetrics(route.Metrics()...)
		debugAPIService.MustRegisterMetrics(retrieve.Metrics()...)

		if apiService != nil {
			debugAPIService.MustRegisterMetrics(apiService.Metrics()...)
		}
		if l, ok := logger.(metrics.Collector); ok {
			debugAPIService.MustRegisterMetrics(l.Metrics()...)
		}

		// if l, ok := settlement.(metrics.Collector); ok {
		//	debugAPIService.MustRegisterMetrics(l.Metrics()...)
		// }

		// inject dependencies and configure full debug api http path routes
		debugAPIService.Configure(p2ps, pingPong, group, kad, lightNodes, bootNodes, storer, route, chunkInfo, retrieve)
		if apiInterface != nil {
			debugAPIService.MustRegisterTraffic(apiInterface)
		}
	}

	if err = kad.Start(p2pCtx); err != nil {
		return nil, err
	}
	if !o.IsDev {
		hiveObj.Start()
	}

	stack, err := NewRPC(logger, Config{
		EnableApiTLS: o.EnableApiTLS,
		TlsCrtFile:   o.TlsCrtFile,
		TlsKeyFile:   o.TlsKeyFile,
		DebugAPIAddr: o.DebugAPIAddr,
		APIAddr:      o.APIAddr,
		//
		DataDir: o.DataDir,
		// HTTPAddr:    o.HTTPAddr,
		// HTTPCors:    o.CORSAllowedOrigins,
		// HTTPModules: []string{"debug", "api"},
		WSAddr:    o.WSAddr,
		WSOrigins: o.CORSAllowedOrigins,
		WSModules: []string{"group", "p2p", "chunkInfo", "traffic", "retrieval", "oracle"},
	})
	if err != nil {
		return nil, err
	}
	stack.RegisterAPIs([]rpc.API{
		group.API(),        // group
		kad.API(),          // p2p
		chunkInfo.API(),    // chunkInfo
		apiInterface.API(), // traffic
		retrieve.API(),     // retrieval
		oracleChain.API(),  // oracle
	})
	if err = stack.Start(); err != nil {
		return nil, err
	}

	if err = p2ps.Ready(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Aurora) Shutdown(ctx context.Context) error {
	errs := new(multiError)

	if b.apiCloser != nil {
		if err := b.apiCloser.Close(); err != nil {
			errs.add(fmt.Errorf("api: %w", err))
		}
	}

	var eg errgroup.Group
	if b.apiServer != nil {
		eg.Go(func() error {
			if err := b.apiServer.Shutdown(ctx); err != nil {
				return fmt.Errorf("api server: %w", err)
			}
			return nil
		})
	}
	if b.debugAPIServer != nil {
		eg.Go(func() error {
			if err := b.debugAPIServer.Shutdown(ctx); err != nil {
				return fmt.Errorf("debug api server: %w", err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		errs.add(err)
	}

	b.p2pCancel()
	if err := b.p2pService.Close(); err != nil {
		errs.add(fmt.Errorf("p2p server: %w", err))
	}

	if c := b.ethClientCloser; c != nil {
		c()
	}

	if err := b.tracerCloser.Close(); err != nil {
		errs.add(fmt.Errorf("tracer: %w", err))
	}

	if err := b.stateStoreCloser.Close(); err != nil {
		errs.add(fmt.Errorf("statestore: %w", err))
	}

	if err := b.localstoreCloser.Close(); err != nil {
		errs.add(fmt.Errorf("localstore: %w", err))
	}

	if b.groupCloser != nil {
		if err := b.groupCloser.Close(); err != nil {
			errs.add(fmt.Errorf("multicast: %w", err))
		}
	}

	if err := b.topologyCloser.Close(); err != nil {
		errs.add(fmt.Errorf("topology driver: %w", err))
	}

	if err := b.errorLogWriter.Close(); err != nil {
		errs.add(fmt.Errorf("error log writer: %w", err))
	}

	// Shutdown the resolver service only if it has been initialized.
	if b.resolverCloser != nil {
		if err := b.resolverCloser.Close(); err != nil {
			errs.add(fmt.Errorf("resolver service: %w", err))
		}
	}

	if errs.hasErrors() {
		return errs
	}

	return nil
}

type multiError struct {
	errors []error
}

func (e *multiError) Error() string {
	if len(e.errors) == 0 {
		return ""
	}
	s := e.errors[0].Error()
	for _, err := range e.errors[1:] {
		s += "; " + err.Error()
	}
	return s
}

func (e *multiError) add(err error) {
	e.errors = append(e.errors, err)
}

func (e *multiError) hasErrors() bool {
	return len(e.errors) > 0
}
