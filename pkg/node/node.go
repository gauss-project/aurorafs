// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package node defines the concept of a Bee node
// by bootstrapping and injecting all necessary
// dependencies.
package node

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/debugapi"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/hive"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/localstore"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/gauss-project/aurorafs/pkg/netstore"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/pingpong"
	"github.com/gauss-project/aurorafs/pkg/resolver/multiresolver"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Bee struct {
	p2pService     io.Closer
	p2pCancel      context.CancelFunc
	apiCloser      io.Closer
	apiServer      *http.Server
	debugAPIServer *http.Server
	resolverCloser io.Closer
	errorLogWriter *io.PipeWriter
	tracerCloser   io.Closer

	stateStoreCloser io.Closer
	localstoreCloser io.Closer
	topologyCloser   io.Closer

	ethClientCloser func()
	//recoveryHandleCleanup func()
}

type Options struct {
	DataDir                  string
	DBCapacity               uint64
	DBOpenFilesLimit         uint64
	DBWriteBufferSize        uint64
	DBBlockCacheCapacity     uint64
	DBDisableSeeksCompaction bool
	APIAddr                  string
	DebugAPIAddr             string
	Addr                     string
	NATAddr                  string
	EnableWS                 bool
	EnableQUIC               bool
	WelcomeMessage           string
	Bootnodes                []string
	OracleEndpoint           string
	CORSAllowedOrigins       []string
	Logger                   logging.Logger
	Standalone               bool
	TracingEnabled           bool
	TracingEndpoint          string
	TracingServiceName       string
	GlobalPinningEnabled     bool
	PaymentThreshold         string
	PaymentTolerance         string
	PaymentEarly             string
	ResolverConnectionCfgs   []multiresolver.ConnectionConfig
	GatewayMode              bool
	BootnodeMode             bool
	SwapEndpoint             string
	SwapFactoryAddress       string
	SwapInitialDeposit       string
	SwapEnable               bool
}

func NewBee(addr string, bosonAddress boson.Address, publicKey ecdsa.PublicKey, signer crypto.Signer, networkID uint64, logger logging.Logger, libp2pPrivateKey, pssPrivateKey *ecdsa.PrivateKey, o Options) (b *Bee, err error) {
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

	b = &Bee{
		p2pCancel:      p2pCancel,
		errorLogWriter: logger.WriterLevel(logrus.ErrorLevel),
		tracerCloser:   tracerCloser,
	}

	var debugAPIService *debugapi.Service
	if o.DebugAPIAddr != "" {
		overlayEthAddress, err := signer.EthereumAddress()
		if err != nil {
			return nil, fmt.Errorf("eth address: %w", err)
		}
		// set up basic debug api endpoints for debugging and /health endpoint
		debugAPIService = debugapi.New(bosonAddress, publicKey, pssPrivateKey.PublicKey, overlayEthAddress, logger, tracer, o.CORSAllowedOrigins)

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
			logger.Infof("debug api address: %s", debugAPIListener.Addr())

			if err := debugAPIServer.Serve(debugAPIListener); err != nil && err != http.ErrServerClosed {
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

	addressbook := addressbook.New(stateStore)

	//var swapBackend *ethclient.Client
	//var overlayEthAddress common.Address
	//var chainID int64
	//var transactionService transaction.Service
	//var chequebookFactory chequebook.Factory
	//var chequebookService chequebook.Service
	//var chequeStore chequebook.ChequeStore
	//var cashoutService chequebook.CashoutService

	//if o.SwapEnable {
	//	swapBackend, overlayEthAddress, chainID, transactionService, err = InitChain(
	//		p2pCtx,
	//		logger,
	//		stateStore,
	//		o.SwapEndpoint,
	//		signer,
	//	)
	//	if err != nil {
	//		return nil, err
	//	}
	//	b.ethClientCloser = swapBackend.Close
	//
	//	chequebookFactory, err = InitChequebookFactory(
	//		logger,
	//		swapBackend,
	//		chainID,
	//		transactionService,
	//		o.SwapFactoryAddress,
	//	)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	if err = chequebookFactory.VerifyBytecode(p2pCtx); err != nil {
	//		return nil, fmt.Errorf("factory fail: %w", err)
	//	}
	//
	//	chequebookService, err = InitChequebookService(
	//		p2pCtx,
	//		logger,
	//		stateStore,
	//		signer,
	//		chainID,
	//		swapBackend,
	//		overlayEthAddress,
	//		transactionService,
	//		chequebookFactory,
	//		o.SwapInitialDeposit,
	//	)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	chequeStore, cashoutService = initChequeStoreCashout(
	//		stateStore,
	//		swapBackend,
	//		chequebookFactory,
	//		chainID,
	//		overlayEthAddress,
	//		transactionService,
	//	)
	//}

	p2ps, err := libp2p.New(p2pCtx, signer, networkID, bosonAddress, addr, addressbook, stateStore, logger, tracer, libp2p.Options{
		PrivateKey:     libp2pPrivateKey,
		NATAddr:        o.NATAddr,
		EnableWS:       o.EnableWS,
		EnableQUIC:     o.EnableQUIC,
		Standalone:     o.Standalone,
		WelcomeMessage: o.WelcomeMessage,
	})
	if err != nil {
		return nil, fmt.Errorf("p2p service: %w", err)
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

	hive := hive.New(p2ps, addressbook, networkID, logger)
	if err = p2ps.AddProtocol(hive.Protocol()); err != nil {
		return nil, fmt.Errorf("hive service: %w", err)
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

	//var settlement settlement.Interface
	//var swapService *swap.Service

	//if o.SwapEnable {
	//	swapService, err = InitSwap(
	//		p2ps,
	//		logger,
	//		stateStore,
	//		networkID,
	//		overlayEthAddress,
	//		chequebookService,
	//		chequeStore,
	//		cashoutService,
	//	)
	//	if err != nil {
	//		return nil, err
	//	}
	//	settlement = swapService
	//} else {
	//	pseudosettleService := pseudosettle.New(p2ps, logger, stateStore)
	//	if err = p2ps.AddProtocol(pseudosettleService.Protocol()); err != nil {
	//		return nil, fmt.Errorf("pseudosettle service: %w", err)
	//	}
	//	settlement = pseudosettleService
	//}

	//paymentThreshold, ok := new(big.Int).SetString(o.PaymentThreshold, 10)
	//if !ok {
	//	return nil, fmt.Errorf("invalid payment threshold: %s", paymentThreshold)
	//}
	//pricing := pricing.New(p2ps, logger, paymentThreshold)
	//if err = p2ps.AddProtocol(pricing.Protocol()); err != nil {
	//	return nil, fmt.Errorf("pricing service: %w", err)
	//}

	//paymentTolerance, ok := new(big.Int).SetString(o.PaymentTolerance, 10)
	//if !ok {
	//	return nil, fmt.Errorf("invalid payment tolerance: %s", paymentTolerance)
	//}
	//paymentEarly, ok := new(big.Int).SetString(o.PaymentEarly, 10)
	//if !ok {
	//	return nil, fmt.Errorf("invalid payment early: %s", paymentEarly)
	//}
	//acc, err := accounting.NewAccounting(
	//	paymentThreshold,
	//	paymentTolerance,
	//	paymentEarly,
	//	logger,
	//	stateStore,
	//	settlement,
	//	pricing,
	//)
	//if err != nil {
	//	return nil, fmt.Errorf("accounting: %w", err)
	//}

	//settlement.SetNotifyPaymentFunc(acc.AsyncNotifyPayment)
	//pricing.SetPaymentThresholdObserver(acc)

	kad := kademlia.New(bosonAddress, addressbook, hive, p2ps, logger, kademlia.Options{Bootnodes: bootnodes, StandaloneMode: o.Standalone, BootnodeMode: o.BootnodeMode})
	b.topologyCloser = kad
	hive.SetAddPeersHandler(kad.AddPeers)
	p2ps.SetPickyNotifier(kad)
	addrs, err := p2ps.Addresses()
	if err != nil {
		return nil, fmt.Errorf("get server addresses: %w", err)
	}

	for _, addr := range addrs {
		logger.Debugf("p2p address: %s", addr)
	}

	route := routetab.New(bosonAddress, p2pCtx, p2ps, kad, stateStore, logger)
	if err = p2ps.AddProtocol(route.Protocol()); err != nil {
		return nil, fmt.Errorf("routetab service: %w", err)
	}

	var path string

	if o.DataDir != "" {
		path = filepath.Join(o.DataDir, "localstore")
	}
	lo := &localstore.Options{
		Capacity:               o.DBCapacity,
		OpenFilesLimit:         o.DBOpenFilesLimit,
		BlockCacheCapacity:     o.DBBlockCacheCapacity,
		WriteBufferSize:        o.DBWriteBufferSize,
		DisableSeeksCompaction: o.DBDisableSeeksCompaction,
	}
	storer, err := localstore.New(path, bosonAddress.Bytes(), lo, logger)
	if err != nil {
		return nil, fmt.Errorf("localstore: %w", err)
	}
	b.localstoreCloser = storer

	retrieve := retrieval.New(bosonAddress, p2ps, kad, storer, logger, tracer)
	if err = p2ps.AddProtocol(retrieve.Protocol()); err != nil {
		return nil, fmt.Errorf("retrieval service: %w", err)
	}

	ns := netstore.New(storer, retrieve, logger)

	traversalService := traversal.NewService(ns)

	chunkInfo := chunkinfo.New(p2ps, logger, traversalService, o.OracleEndpoint)
	if err = p2ps.AddProtocol(chunkInfo.Protocol()); err != nil {
		return nil, fmt.Errorf("chunkInfo service: %w", err)
	}

	retrieve.Config(chunkInfo)


	multiResolver := multiresolver.NewMultiResolver(
		multiresolver.WithConnectionConfigs(o.ResolverConnectionCfgs),
		multiresolver.WithLogger(o.Logger),
	)
	b.resolverCloser = multiResolver

	var apiService api.Service
	if o.APIAddr != "" {
		// API server

		apiService = api.New(ns, multiResolver, bosonAddress, chunkInfo, traversalService, logger, tracer, api.Options{
			CORSAllowedOrigins: o.CORSAllowedOrigins,
			GatewayMode:        o.GatewayMode,
			WsPingPeriod:       60 * time.Second,
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
			logger.Infof("api address: %s", apiListener.Addr())

			if err := apiServer.Serve(apiListener); err != nil && err != http.ErrServerClosed {
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
		//debugAPIService.MustRegisterMetrics(acc.Metrics()...)
		debugAPIService.MustRegisterMetrics(storer.Metrics()...)

		debugAPIService.MustRegisterMetrics(retrieve.Metrics()...)

		if apiService != nil {
			debugAPIService.MustRegisterMetrics(apiService.Metrics()...)
		}
		if l, ok := logger.(metrics.Collector); ok {
			debugAPIService.MustRegisterMetrics(l.Metrics()...)
		}

		//if l, ok := settlement.(metrics.Collector); ok {
		//	debugAPIService.MustRegisterMetrics(l.Metrics()...)
		//}

		// inject dependencies and configure full debug api http path routes
		debugAPIService.Configure(p2ps, pingPong, kad, storer)
	}

	if err := kad.Start(p2pCtx); err != nil {
		return nil, err
	}
	p2ps.Ready()

	return b, nil
}

func (b *Bee) Shutdown(ctx context.Context) error {
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
