package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	optionNameDataDir                  = "data-dir"
	optionNameCacheCapacity            = "cache-capacity"
	optionNameDBOpenFilesLimit         = "db-open-files-limit"
	optionNameDBBlockCacheCapacity     = "db-block-cache-capacity"
	optionNameDBWriteBufferSize        = "db-write-buffer-size"
	optionNameDBDisableSeeksCompaction = "db-disable-seeks-compaction"
	optionNamePassword                 = "password"
	optionNamePasswordFile             = "password-file"
	optionNameAPIAddr                  = "api-addr"
	optionNameP2PAddr                  = "p2p-addr"
	optionNameNATAddr                  = "nat-addr"
	optionNameP2PWSEnable              = "p2p-ws-enable"
	optionNameP2PQUICEnable            = "p2p-quic-enable"
	optionNameDebugAPIEnable           = "debug-api-enable"
	optionNameDebugAPIAddr             = "debug-api-addr"
	optionNameBootnodes                = "bootnode"
	optionNameOracleEndpoint           = "oracle-endpoint"
	optionNameOracleContractAddr       = "oracle-contract-addr"
	optionNameNetworkID                = "network-id"
	optionWelcomeMessage               = "welcome-message"
	optionCORSAllowedOrigins           = "cors-allowed-origins"
	optionNameStandalone               = "standalone"
	optionNameDevMode                  = "dev-mode"
	optionNameTracingEnabled           = "tracing-enable"
	optionNameTracingEndpoint          = "tracing-endpoint"
	optionNameTracingServiceName       = "tracing-service-name"
	optionNameVerbosity                = "verbosity"
	optionNameGlobalPinningEnabled     = "global-pinning-enable"
	optionNameApiFileBufferMultiple    = "api-file-buffer-multiple"
	//optionNamePaymentThreshold          = "payment-threshold"
	//optionNamePaymentTolerance          = "payment-tolerance"
	//optionNamePaymentEarly              = "payment-early"
	optionNameResolverEndpoints   = "resolver-options"
	optionNameBootnodeMode        = "bootnode-mode"
	optionNameFullNode            = "full-node"
	optionNameGatewayMode         = "gateway-mode"
	optionNameTrafficContractAddr = "traffic-contract-addr"
	optionNameTrafficEnable       = "traffic-enable"
	optionNameBinMaxPeers         = "bin-max-peers"
	optionNameLightMaxPeers       = "light-max-peers"
	optionNameAllowPrivateCIDRs   = "allow-private-cidrs"
	optionNameRestrictedAPI      = "restricted"
	optionNameTokenEncryptionKey = "token-encryption-key"
	optionNameAdminPasswordHash  = "admin-password"
)

func init() {
	cobra.EnableCommandSorting = false
}

type command struct {
	root           *cobra.Command
	config         *viper.Viper
	passwordReader passwordReader
	cfgFile        string
	homeDir        string
}

type option func(*command)

func newCommand(opts ...option) (c *command, err error) {
	c = &command{
		root: &cobra.Command{
			Use:           "aurora",
			Short:         "aurora file system node",
			SilenceErrors: true,
			SilenceUsage:  true,
			PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
				return c.initConfig()
			},
		},
	}

	for _, o := range opts {
		o(c)
	}
	if c.passwordReader == nil {
		c.passwordReader = new(stdInPasswordReader)
	}

	// Find home directory.
	if err := c.setHomeDir(); err != nil {
		return nil, err
	}

	c.initGlobalFlags()

	if err := c.initStartCmd(); err != nil {
		return nil, err
	}

	if err := c.initInitCmd(); err != nil {
		return nil, err
	}

	//if err := c.initDeployCmd(); err != nil {
	//	return nil, err
	//}

	c.initVersionCmd()
	c.initDBCmd()

	if err := c.initConfigurateOptionsCmd(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *command) Execute() (err error) {
	return c.root.Execute()
}

// Execute parses command line arguments and runs appropriate functions.
func Execute() (err error) {
	c, err := newCommand()
	if err != nil {
		return err
	}
	return c.Execute()
}

func (c *command) initGlobalFlags() {
	globalFlags := c.root.PersistentFlags()
	globalFlags.StringVar(&c.cfgFile, "config", "", "config file (default is $HOME/.aurorafs.yaml)")
}

func (c *command) initConfig() (err error) {
	config := viper.New()
	configName := ".aurorafs"
	if c.cfgFile != "" {
		// Use config file from the flag.
		config.SetConfigFile(c.cfgFile)
	} else {
		// Search config in home directory with name ".aurorafs" (without extension).
		config.AddConfigPath(c.homeDir)
		config.SetConfigName(configName)
	}

	// Environment
	config.SetEnvPrefix("aurorafs")
	config.AutomaticEnv() // read in environment variables that match
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if c.homeDir != "" && c.cfgFile == "" {
		c.cfgFile = filepath.Join(c.homeDir, configName+".yaml")
	}

	// If a config file is found, read it in.
	if err := config.ReadInConfig(); err != nil {
		var e viper.ConfigFileNotFoundError
		if !errors.As(err, &e) {
			return err
		}
	}
	c.config = config
	return nil
}

func (c *command) setHomeDir() (err error) {
	if c.homeDir != "" {
		return
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	c.homeDir = dir
	return nil
}

func (c *command) setAllFlags(cmd *cobra.Command) {
	cmd.Flags().String(optionNameDataDir, filepath.Join(c.homeDir, ".aurorafs"), "data directory")
	cmd.Flags().Uint64(optionNameCacheCapacity, 80000, fmt.Sprintf("cache capacity in chunks, multiply by %d to get approximate capacity in bytes", boson.ChunkSize))
	cmd.Flags().Uint64(optionNameDBOpenFilesLimit, 1024, "number of open files allowed by database")
	cmd.Flags().Uint64(optionNameDBBlockCacheCapacity, 32*1024*1024, "size of block cache of the database in bytes")
	cmd.Flags().Uint64(optionNameDBWriteBufferSize, 32*1024*1024, "size of the database write buffer in bytes")
	cmd.Flags().Bool(optionNameDBDisableSeeksCompaction, false, "disables db compactions triggered by seeks")
	cmd.Flags().String(optionNamePassword, "", "password for decrypting keys")
	cmd.Flags().String(optionNamePasswordFile, "", "path to a file that contains password for decrypting keys")
	cmd.Flags().String(optionNameAPIAddr, ":1633", "HTTP API listen address")
	cmd.Flags().String(optionNameP2PAddr, ":1634", "P2P listen address")
	cmd.Flags().String(optionNameNATAddr, "", "NAT exposed address")
	cmd.Flags().Bool(optionNameP2PWSEnable, false, "enable P2P WebSocket transport")
	cmd.Flags().Bool(optionNameP2PQUICEnable, false, "enable P2P QUIC transport")
	cmd.Flags().StringSlice(optionNameBootnodes, []string{}, "initial nodes to connect to")
	cmd.Flags().String(optionNameOracleEndpoint, "", "link to oracle endpoint")
	cmd.Flags().String(optionNameOracleContractAddr, "", "link to oracle contract")
	cmd.Flags().Bool(optionNameDebugAPIEnable, true, "enable debug HTTP API")
	cmd.Flags().String(optionNameDebugAPIAddr, ":1635", "debug HTTP API listen address")
	cmd.Flags().Uint64(optionNameNetworkID, 9, "ID of the Aurora network")
	cmd.Flags().StringSlice(optionCORSAllowedOrigins, []string{"https://www.aufs.io"}, "origins with CORS headers enabled")
	cmd.Flags().Bool(optionNameStandalone, false, "whether we want the node to start with no listen addresses for p2p")
	cmd.Flags().Bool(optionNameDevMode, false, "run dev mode")
	cmd.Flags().Bool(optionNameTracingEnabled, false, "enable tracing")
	cmd.Flags().String(optionNameTracingEndpoint, "127.0.0.1:6831", "endpoint to send tracing data")
	cmd.Flags().String(optionNameTracingServiceName, "aurorafs", "service name identifier for tracing")
	cmd.Flags().String(optionNameVerbosity, "info", "log verbosity level 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=trace")
	cmd.Flags().String(optionWelcomeMessage, "", "send a welcome message string during handshakes")
	cmd.Flags().Bool(optionNameGlobalPinningEnabled, false, "enable global pinning")
	cmd.Flags().Int(optionNameApiFileBufferMultiple, 8, "When the API downloads files, the multiple of the buffer (256kb for files less than 10mb and 512kb for others), the default multiple is 8")
	//cmd.Flags().String(optionNamePaymentThreshold, "10000000000000", "threshold in BZZ where you expect to get paid from your peers")
	//cmd.Flags().String(optionNamePaymentTolerance, "50000000000000", "excess debt above payment threshold in BZZ where you disconnect from your peer")
	//cmd.Flags().String(optionNamePaymentEarly, "1000000000000", "amount in BZZ below the peers payment threshold when we initiate settlement")
	cmd.Flags().StringSlice(optionNameResolverEndpoints, []string{}, "ENS compatible API endpoint for a TLD and with contract address, can be repeated, format [tld:][contract-addr@]url")
	cmd.Flags().Bool(optionNameGatewayMode, false, "disable a set of sensitive features in the api")
	cmd.Flags().Bool(optionNameBootnodeMode, false, "cause the node to always accept incoming connections")
	cmd.Flags().String(optionNameTrafficContractAddr, "", "link to traffic contract")
	cmd.Flags().Bool(optionNameTrafficEnable, false, "enable traffic")
	cmd.Flags().Bool(optionNameFullNode, true, "full node")
	cmd.Flags().Int(optionNameLightMaxPeers, 100, "connected light node max limit")
	cmd.Flags().Int(optionNameBinMaxPeers, 20, "kademlia every k bucket connected peers max limit")
	cmd.Flags().Bool(optionNameAllowPrivateCIDRs, false, "allow to advertise private CIDRs to the public network")
	cmd.Flags().Bool(optionNameRestrictedAPI, false, "enable permission check on the http APIs")
	cmd.Flags().String(optionNameTokenEncryptionKey, "", "admin username to get the security token")
	cmd.Flags().String(optionNameAdminPasswordHash, "", "bcrypt hash of the admin password to get the security token")
}

func newLogger(cmd *cobra.Command, verbosity string) (logging.Logger, error) {
	var logger logging.Logger
	switch verbosity {
	case "0", "silent":
		logger = logging.New(io.Discard, 0)
	case "1", "error":
		logger = logging.New(cmd.OutOrStdout(), logrus.ErrorLevel)
	case "2", "warn":
		logger = logging.New(cmd.OutOrStdout(), logrus.WarnLevel)
	case "3", "info":
		logger = logging.New(cmd.OutOrStdout(), logrus.InfoLevel)
	case "4", "debug":
		logger = logging.New(cmd.OutOrStdout(), logrus.DebugLevel)
	case "5", "trace":
		logger = logging.New(cmd.OutOrStdout(), logrus.TraceLevel)
	default:
		return nil, fmt.Errorf("unknown verbosity level %q", verbosity)
	}
	return logger, nil
}
