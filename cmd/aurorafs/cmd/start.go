package cmd

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	_ "embed"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/keystore"
	filekeystore "github.com/gauss-project/aurorafs/pkg/keystore/file"
	memkeystore "github.com/gauss-project/aurorafs/pkg/keystore/mem"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/node"
	"github.com/gauss-project/aurorafs/pkg/resolver/multiresolver"
	"github.com/kardianos/service"
	"github.com/spf13/cobra"
)

const (
	serviceName = "AuroraSvc"
)

//go:embed aurora-welcome-message.txt
var auroraWelcomeMessage string

func (c *command) initStartCmd() (err error) {

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a Aurora node",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) > 0 {
				return cmd.Help()
			}

			v := strings.ToLower(c.config.GetString(optionNameVerbosity))
			logger, err := newLogger(cmd, v)
			if err != nil {
				return fmt.Errorf("new logger: %v", err)
			}

			go startTimeBomb(logger)

			isWindowsService, err := isWindowsService()
			if err != nil {
				return fmt.Errorf("failed to determine if we are running in service: %w", err)
			}

			if isWindowsService {
				var err error
				logger, err = createWindowsEventLogger(serviceName, logger)
				if err != nil {
					return fmt.Errorf("failed to create windows logger %w", err)
				}
			}

			// If the resolver is specified, resolve all connection strings
			// and fail on any errors.
			var resolverCfgs []multiresolver.ConnectionConfig
			resolverEndpoints := c.config.GetStringSlice(optionNameResolverEndpoints)
			if len(resolverEndpoints) > 0 {
				resolverCfgs, err = multiresolver.ParseConnectionStrings(resolverEndpoints)
				if err != nil {
					return err
				}
			}

			fmt.Print(auroraWelcomeMessage)

			fmt.Printf("\n\nversion: %v - planned to be supported until %v, please follow https://aufs.io/\n\n", aufs.Version, endSupportDate())

			debugAPIAddr := c.config.GetString(optionNameDebugAPIAddr)
			if !c.config.GetBool(optionNameDebugAPIEnable) {
				debugAPIAddr = ""
			}

			signerConfig, err := c.configureSigner(cmd, logger)
			if err != nil {
				return err
			}

			logger.Infof("version: %v", aufs.Version)

			bootNode := c.config.GetBool(optionNameBootnodeMode)
			fullNode := c.config.GetBool(optionNameFullNode)
			mode := aurora.NewModel()
			if bootNode {
				mode.SetMode(aurora.BootNode)
				mode.SetMode(aurora.FullNode)
				logger.Info("Start node mode boot.")
			} else if fullNode {
				mode.SetMode(aurora.FullNode)
				logger.Info("Start node mode full.")
			} else {
				logger.Info("Start node mode light.")
			}

			chainEndpoint := func() string {
				p := c.config.GetString(optionNameChainEndpoint)
				if p != "" {
					return p
				}
				return c.config.GetString(optionNameOracleEndpoint)
			}()

			b, err := node.NewAurora(mode, c.config.GetString(optionNameP2PAddr), signerConfig.address, *signerConfig.publicKey, signerConfig.signer, c.config.GetUint64(optionNameNetworkID), logger, signerConfig.libp2pPrivateKey, node.Options{
				DataDir:               c.config.GetString(optionNameDataDir),
				CacheCapacity:         c.config.GetUint64(optionNameCacheCapacity),
				DBDriver:              c.config.GetString(optionDatabaseDriver),
				DBPath:                c.config.GetString(optionDatabasePath),
				HTTPAddr:              c.config.GetString(optionNameHTTPAddr),
				WSAddr:                c.config.GetString(optionNameWebsocketAddr),
				APIAddr:               c.config.GetString(optionNameAPIAddr),
				DebugAPIAddr:          debugAPIAddr,
				ApiBufferSizeMul:      c.config.GetInt(optionNameApiFileBufferMultiple),
				NATAddr:               c.config.GetString(optionNameNATAddr),
				EnableWS:              c.config.GetBool(optionNameP2PWSEnable),
				EnableQUIC:            c.config.GetBool(optionNameP2PQUICEnable),
				WelcomeMessage:        c.config.GetString(optionWelcomeMessage),
				Bootnodes:             c.config.GetStringSlice(optionNameBootnodes),
				ChainEndpoint:         chainEndpoint,
				OracleContractAddress: c.config.GetString(optionNameOracleContractAddr),
				CORSAllowedOrigins:    c.config.GetStringSlice(optionCORSAllowedOrigins),
				Standalone:            c.config.GetBool(optionNameStandalone),
				IsDev:                 c.config.GetBool(optionNameDevMode),
				TracingEnabled:        c.config.GetBool(optionNameTracingEnabled),
				TracingEndpoint:       c.config.GetString(optionNameTracingEndpoint),
				TracingServiceName:    c.config.GetString(optionNameTracingServiceName),
				Logger:                logger,
				// GlobalPinningEnabled:     c.config.GetBool(optionNameGlobalPinningEnabled),
				// PaymentThreshold:         c.config.GetString(optionNamePaymentThreshold),
				// PaymentTolerance:         c.config.GetString(optionNamePaymentTolerance),
				// PaymentEarly:             c.config.GetString(optionNamePaymentEarly),
				ResolverConnectionCfgs: resolverCfgs,
				GatewayMode:            c.config.GetBool(optionNameGatewayMode),
				TrafficEnable:          c.config.GetBool(optionNameTrafficEnable),
				TrafficContractAddr:    c.config.GetString(optionNameTrafficContractAddr),
				KadBinMaxPeers:         c.config.GetInt(optionNameBinMaxPeers),
				LightNodeMaxPeers:      c.config.GetInt(optionNameLightMaxPeers),
				AllowPrivateCIDRs:      c.config.GetBool(optionNameAllowPrivateCIDRs),
				Restricted:             c.config.GetBool(optionNameRestrictedAPI),
				TokenEncryptionKey:     c.config.GetString(optionNameTokenEncryptionKey),
				AdminPasswordHash:      c.config.GetString(optionNameAdminPasswordHash),
				RouteAlpha:             c.config.GetInt32(optionNameRouteAlpha),
				Groups:                 c.config.Get(optionNameGroups),
				EnableApiTLS:           c.config.GetBool(optionNameEnableApiTls),
				TlsCrtFile:             c.config.GetString(optionNameTlsCRT),
				TlsKeyFile:             c.config.GetString(optionNameTlsKey),
			})
			if err != nil {
				return err
			}

			// Wait for termination or interrupt signals.
			// We want to clean up things at the end.
			interruptChannel := make(chan os.Signal, 1)
			signal.Notify(interruptChannel, syscall.SIGINT, syscall.SIGTERM)

			p := &program{
				start: func() {
					// Block main goroutine until it is interrupted
					sig := <-interruptChannel

					logger.Debugf("received signal: %v", sig)
					logger.Info("shutting down")
				},
				stop: func() {
					// Shutdown
					done := make(chan struct{})
					go func() {
						defer close(done)

						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						defer cancel()

						if err := b.Shutdown(ctx); err != nil {
							logger.Errorf("shutdown: %v", err)
						}
					}()

					// If shutdown function is blocking too long,
					// allow process termination by receiving another signal.
					select {
					case sig := <-interruptChannel:
						logger.Debugf("received signal: %v", sig)
					case <-done:
					}
				},
			}

			if isWindowsService {
				s, err := service.New(p, &service.Config{
					Name:        serviceName,
					DisplayName: "Aurora",
					Description: "Aurora client.",
				})
				if err != nil {
					return err
				}

				if err = s.Run(); err != nil {
					return err
				}
			} else {
				// start blocks until some interrupt is received
				p.start()
				p.stop()
			}

			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return c.config.BindPFlags(cmd.Flags())
		},
	}

	c.setAllFlags(cmd)
	c.root.AddCommand(cmd)
	return nil
}

type program struct {
	start func()
	stop  func()
}

func (p *program) Start(s service.Service) error {
	// Start should not block. Do the actual work async.
	go p.start()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.stop()
	return nil
}

type signerConfig struct {
	signer           crypto.Signer
	address          boson.Address
	publicKey        *ecdsa.PublicKey
	libp2pPrivateKey *ecdsa.PrivateKey
}

// func waitForClef(logger logging.Logger, maxRetries uint64, endpoint string) (externalSigner *external.ExternalSigner, err error) {
//	for {
//		externalSigner, err = external.NewExternalSigner(endpoint)
//		if err == nil {
//			return externalSigner, nil
//		}
//		if maxRetries == 0 {
//			return nil, err
//		}
//		maxRetries--
//		logger.Warningf("failing to connect to clef signer: %v", err)
//
//		time.Sleep(5 * time.Second)
//	}
// }

func (c *command) configureSigner(cmd *cobra.Command, logger logging.Logger) (config *signerConfig, err error) {
	var kt keystore.Service
	if c.config.GetString(optionNameDataDir) == "" {
		kt = memkeystore.New()
		logger.Warning("data directory not provided, keys are not persisted")
	} else {
		kt = filekeystore.New(filepath.Join(c.config.GetString(optionNameDataDir), "keys"))
	}

	var signer crypto.Signer
	var address boson.Address
	var password string
	var publicKey *ecdsa.PublicKey
	if p := c.config.GetString(optionNamePassword); p != "" {
		password = p
	} else if pf := c.config.GetString(optionNamePasswordFile); pf != "" {
		b, err := os.ReadFile(pf)
		if err != nil {
			return nil, err
		}
		password = string(bytes.Trim(b, "\n"))
	} else {
		// if libp2p key exists we can assume all required keys exist
		// so prompt for a password to unlock them
		// otherwise prompt for new password with confirmation to create them
		exists, err := kt.Exists("libp2p")
		if err != nil {
			return nil, err
		}
		if exists {
			password, err = terminalPromptPassword(cmd, c.passwordReader, "Password")
			if err != nil {
				return nil, err
			}
		} else {
			password, err = terminalPromptCreatePassword(cmd, c.passwordReader)
			if err != nil {
				return nil, err
			}
		}
	}

	// if c.config.GetBool(optionNameClefSignerEnable) {
	//	endpoint := c.config.GetString(optionNameClefSignerEndpoint)
	//	if endpoint == "" {
	//		endpoint, err = clef.DefaultIpcPath()
	//		if err != nil {
	//			return nil, err
	//		}
	//	}
	//
	//	externalSigner, err := waitForClef(logger, 5, endpoint)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	clefRPC, err := rpc.Dial(endpoint)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	wantedAddress := c.config.GetString(optionNameClefSignerEthereumAddress)
	//	var overlayEthAddress *common.Address = nil
	//	// if wantedAddress was specified use that, otherwise clef account 0 will be selected.
	//	if wantedAddress != "" {
	//		ethAddress := common.HexToAddress(wantedAddress)
	//		overlayEthAddress = &ethAddress
	//	}
	//
	//	signer, err = clef.NewSigner(externalSigner, clefRPC, crypto.Recover, overlayEthAddress)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	publicKey, err = signer.PublicKey()
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	address, err = crypto.NewOverlayAddress(*publicKey, c.config.GetUint64(optionNameNetworkID))
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	logger.Infof("using boson network address through clef: %s", address)
	// } else {
	//	logger.Warning("clef is not enabled; portability and security of your keys is sub optimal")
	PrivateKey, created, err := kt.Key("boson", password)
	if err != nil {
		return nil, fmt.Errorf("boson key: %w", err)
	}
	signer = crypto.NewDefaultSigner(PrivateKey)
	publicKey = &PrivateKey.PublicKey

	address, err = crypto.NewOverlayAddress(*publicKey, c.config.GetUint64(optionNameNetworkID))
	if err != nil {
		return nil, err
	}

	if created {
		logger.Infof("new boson network address created: %s", address)
	} else {
		logger.Infof("using existing boson network address: %s", address)
	}
	// }

	logger.Infof("boson public key %x", crypto.EncodeSecp256k1PublicKey(publicKey))

	libp2pPrivateKey, created, err := kt.Key("libp2p", password)
	if err != nil {
		return nil, fmt.Errorf("libp2p key: %w", err)
	}
	if created {
		logger.Debugf("new libp2p key created")
	} else {
		logger.Debugf("using existing libp2p key")
	}

	return &signerConfig{
		signer:           signer,
		address:          address,
		publicKey:        publicKey,
		libp2pPrivateKey: libp2pPrivateKey,
	}, nil
}
