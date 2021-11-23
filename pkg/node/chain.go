package node

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/oracle"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transacton Service to interact with it using the provided signer.
func InitChain(
	ctx context.Context,
	logger logging.Logger,
	endpoint string,
	stateStore storage.StateStorer,
	signer crypto.Signer,
	oracleContractAddress string,
) (chain.Resolver, error) {

	backend, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial eth client: %w", err)
	}

	chainID, err := backend.ChainID(ctx)
	if err != nil {
		logger.Infof("could not connect to backend at %v. In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --swap-endpoint.", endpoint)
		return nil, fmt.Errorf("get chain id: %w", err)
	}
	if oracleContractAddress == "" {
		return nil, fmt.Errorf("oracle contract address is empty")
	}
	oracleServer, err := oracle.NewServer(logger, backend, oracleContractAddress)
	if err != nil {
		return nil, fmt.Errorf("new oracle service: %w", err)
	}

	// todo enable
	trafficServer, err := traffic.NewServer(logger, backend, oracleContractAddress)
	if err != nil {
		return nil, fmt.Errorf("new traffic service: %w", err)
	}
	transactionService, err := transaction.NewService(logger, backend, signer, stateStore, chainID)
	if err != nil {
		return nil, fmt.Errorf("new transaction service: %w", err)
	}
	return oracleServer, nil
}
