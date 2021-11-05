package mobile

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"os"
	"time"

	aufs "github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	filekeystore "github.com/gauss-project/aurorafs/pkg/keystore/file"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/node"
	"github.com/sirupsen/logrus"
)

type Node struct {
	node   *node.Aurora
	opts   *Options
	logger logging.Logger
}

type signerConfig struct {
	signer           crypto.Signer
	address          boson.Address
	publicKey        *ecdsa.PublicKey
	libp2pPrivateKey *ecdsa.PrivateKey
}

func NewNode(o *Options) (*Node, error) {
	logger, err := newLogger(o.Verbosity)
	if err != nil {
		return nil, err
	}

	signerConfig, err := configureSigner(o.KeysPath, o.Password, o.NetworkID, logger)
	if err != nil {
		return nil, err
	}

	logger.Infof("version: %v", aufs.Version)

	if o.EnableFullNode {
		logger.Info("start node mode full.")
	} else {
		logger.Info("start node mode light.")
	}

	config := o.Export()
	p2pAddr := fmt.Sprintf("%s:%d", ListenAddress, o.P2PPort)

	aurora, err := node.NewAurora(p2pAddr, signerConfig.address, *signerConfig.publicKey, signerConfig.signer, o.NetworkID, logger, signerConfig.libp2pPrivateKey, config)
	if err != nil {
		return nil, err
	}

	return &Node{node: aurora, opts: o, logger: logger}, nil
}

func (n *Node) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return n.node.Shutdown(ctx)
}

func configureSigner(path, password string, networkID uint64, logger logging.Logger) (*signerConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("keystore directory not provided")
	}

	keystore := filekeystore.New(path)

	PrivateKey, created, err := keystore.Key("boson", password)
	if err != nil {
		return nil, fmt.Errorf("boson key: %w", err)
	}
	signer := crypto.NewDefaultSigner(PrivateKey)
	publicKey := &PrivateKey.PublicKey

	address, err := crypto.NewOverlayAddress(*publicKey, networkID)
	if err != nil {
		return nil, err
	}
	if created {
		logger.Infof("new boson network address created: %s", address)
	} else {
		logger.Infof("using existing boson network address: %s", address)
	}

	logger.Infof("boson public key %x", crypto.EncodeSecp256k1PublicKey(publicKey))

	libp2pPrivateKey, created, err := keystore.Key("libp2p", password)
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

func cmdOutput() io.Writer {
	return os.Stdout
}

func newLogger(verbosity string) (logging.Logger, error) {
	var logger logging.Logger
	switch verbosity {
	case "0", "silent":
		logger = logging.New(io.Discard, 0)
	case "1", "error":
		logger = logging.New(cmdOutput(), logrus.ErrorLevel)
	case "2", "warn":
		logger = logging.New(cmdOutput(), logrus.WarnLevel)
	case "3", "info":
		logger = logging.New(cmdOutput(), logrus.InfoLevel)
	case "4", "debug":
		logger = logging.New(cmdOutput(), logrus.DebugLevel)
	case "5", "trace":
		logger = logging.New(cmdOutput(), logrus.TraceLevel)
	default:
		return nil, fmt.Errorf("unknown verbosity level %q", verbosity)
	}

	return logger, nil
}
