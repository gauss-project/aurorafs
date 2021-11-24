package node

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"

	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/oracle"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
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
	trafficEnable bool,
	trafficContractAddr string,
	beneficiary common.Address,
	p2pService p2p.Service,
	cheque chequePkg.Cheque,

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

	if !trafficEnable {
		// todo
	}

	_, err = chainTraffic.NewServer(logger, backend, oracleContractAddress)
	if err != nil {
		return nil, fmt.Errorf("new traffic service: %w", err)
	}

	transactionService, err := transaction.NewService(logger, backend, signer, stateStore, chainID)
	if err != nil {
		return nil, fmt.Errorf("new transaction service: %w", err)
	}

	InitTraffic(stateStore, chainID.Int64(), beneficiary, transactionService, logger, cheque, p2pService)

	return oracleServer, nil
}

func InitTraffic(store storage.StateStorer, chainID int64, beneficiary common.Address, transactionService transaction.Service, logger logging.Logger, cheque chequePkg.Cheque, p2pService p2p.Service) *traffic.Service {
	chequeStore := chequePkg.NewChequeStore(
		store,
		beneficiary,
		chequePkg.RecoverCheque,
	)

	cashOut := chequePkg.NewCashoutService(
		store,
		transactionService,
		chequeStore,
	)

	trafficService := traffic.New(logger, beneficiary, store, chequeStore, cheque, cashOut, p2pService)

	return trafficService
}
