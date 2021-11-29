package node

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/oracle"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	"github.com/gauss-project/aurorafs/pkg/settlement/pseudosettle"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transacton Service to interact with it using the provided signer.
func InitChain(
	ctx context.Context,
	logger logging.Logger,
	endpoint string,
	oracleContractAddress string,
	stateStore storage.StateStorer,
	signer crypto.Signer,
	trafficEnable bool,
	trafficContractAddr string,
	p2pService *libp2p.Service) (chain.Resolver, settlement.Interface, error) {

	backend, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("dial eth client: %w", err)
	}

	chainID, err := backend.ChainID(ctx)
	if err != nil {
		logger.Infof("could not connect to backend at %v. In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --swap-endpoint.", endpoint)
		return nil, nil, fmt.Errorf("get chain id: %w", err)
	}
	if oracleContractAddress == "" {
		return nil, nil, fmt.Errorf("oracle contract address is empty")
	}
	oracleServer, err := oracle.NewServer(logger, backend, oracleContractAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("new oracle service: %w", err)
	}

	if !trafficEnable {
		return oracleServer, pseudosettle.New(p2pService, logger, stateStore), nil
	}
	beneficiary, err := signer.EthereumAddress()
	if err != nil {
		return nil, nil, fmt.Errorf("chain address: %w", err)
	}
	trafficChainService, err := chainTraffic.NewServer(logger, backend, trafficContractAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("new traffic service: %w", err)
	}

	transactionService, err := transaction.NewService(logger, backend, signer, stateStore, chainID)
	if err != nil {
		return nil, nil, fmt.Errorf("new transaction service: %w", err)
	}

	trafficService := InitTraffic(stateStore, beneficiary, trafficChainService, transactionService, logger, p2pService, signer, chainID.Int64())
	err = trafficService.InitTraffic()
	if err != nil {
		return nil, nil, fmt.Errorf("InitChain: %w", err)
	}
	return oracleServer, trafficService, nil
}

func InitTraffic(store storage.StateStorer, beneficiary common.Address, trafficChainService chain.Traffic,
	transactionService chain.Transaction, logger logging.Logger, p2pService *libp2p.Service, signer crypto.Signer, chainID int64) *traffic.Service {
	chequeStore := chequePkg.NewChequeStore(store, beneficiary, chequePkg.RecoverCheque, chainID)
	cashOut := chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := traffic.NewAddressBook(store)
	protocol := trafficprotocol.New(p2pService, logger)
	chequeSigner := chequePkg.NewChequeSigner(signer, chainID)
	trafficService := traffic.New(logger, beneficiary, store, trafficChainService, chequeStore, cashOut, p2pService, addressBook, chequeSigner, protocol, chainID)
	protocol.SetTraffic(trafficService)
	return trafficService
}
