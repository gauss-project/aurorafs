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
	p2pService *libp2p.Service) (chain.Resolver, settlement.Interface, traffic.ApiInterface, error) {
	var settlement settlement.Interface
	var apiInterface traffic.ApiInterface
	backend, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial eth client: %w", err)
	}

	chainID, err := backend.ChainID(ctx)
	if err != nil {
		logger.Infof("could not connect to backend at %v. In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --traffic-endpoint.", endpoint)
		return nil, nil, nil, fmt.Errorf("get chain id: %w", err)
	}
	if oracleContractAddress == "" {
		return nil, nil, nil, fmt.Errorf("oracle contract address is empty")
	}
	oracleServer, err := oracle.NewServer(logger, backend, oracleContractAddress)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new oracle service: %w", err)
	}

	if !trafficEnable {
		protocol := pseudosettle.New(p2pService, logger, stateStore)
		if err := p2pService.AddProtocol(protocol.Protocol()); err != nil {
			return nil, nil, nil, fmt.Errorf("traffic local server :%v", err)
		}
		service := pseudosettle.New(p2pService, logger, stateStore)
		if err = service.InitTraffic(); err != nil {
			return nil, nil, nil, fmt.Errorf("InitTraffic:: %w", err)
		}
		settlement = service
		apiInterface = service
		return oracleServer, settlement, apiInterface, nil
	}
	beneficiary, err := signer.EthereumAddress()
	logger.Infof("address  %s", beneficiary.String())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("chain address: %w", err)
	}
	trafficChainService, err := chainTraffic.NewServer(logger, backend, trafficContractAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new traffic service: %w", err)
	}

	transactionService, err := transaction.NewService(logger, backend, signer, stateStore, chainID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new transaction service: %w", err)
	}

	trafficService, err := InitTraffic(stateStore, beneficiary, trafficChainService, transactionService, logger, p2pService, signer, chainID.Int64(), trafficContractAddr)
	if err != nil {
		return nil, nil, nil, err
	}
	err = trafficService.Init()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("InitChain: %w", err)
	}
	settlement = trafficService
	apiInterface = trafficService

	return oracleServer, settlement, apiInterface, nil
}

func InitTraffic(store storage.StateStorer, beneficiary common.Address, trafficChainService chain.Traffic,
	transactionService chain.Transaction, logger logging.Logger, p2pService *libp2p.Service, signer crypto.Signer, chainID int64, trafficContractAddr string) (*traffic.Service, error) {
	chequeStore := chequePkg.NewChequeStore(store, beneficiary, chequePkg.RecoverCheque, chainID)
	cashOut := chequePkg.NewCashoutService(store, transactionService, chequeStore, common.HexToAddress(trafficContractAddr))
	addressBook := traffic.NewAddressBook(store)
	protocol := trafficprotocol.New(p2pService, logger, beneficiary)
	if err := p2pService.AddProtocol(protocol.Protocol()); err != nil {
		return nil, fmt.Errorf("traffic server :%v", err)
	}
	chequeSigner := chequePkg.NewChequeSigner(signer, chainID)
	trafficService := traffic.New(logger, beneficiary, store, trafficChainService, chequeStore, cashOut, p2pService, addressBook, chequeSigner, protocol, chainID)
	protocol.SetTraffic(trafficService)
	return trafficService, nil
}
