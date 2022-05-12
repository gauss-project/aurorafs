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
	chainCommon "github.com/gauss-project/aurorafs/pkg/settlement/chain/common"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/oracle"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	"github.com/gauss-project/aurorafs/pkg/settlement/pseudosettle"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
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
	p2pService *libp2p.Service,
	subPub subscribe.SubPub,
) (chain.Resolver, settlement.Interface, traffic.ApiInterface, chain.Common, error) {
	backend, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("dial eth client: %w", err)
	}

	chainID, err := backend.ChainID(ctx)
	if err != nil {
		logger.Infof("could not connect to backend at %v. In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --traffic-endpoint.", endpoint)
		return nil, nil, nil, nil, fmt.Errorf("get chain id: %w", err)
	}
	cc, err := chainCommon.New(logger, signer, chainID, endpoint)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("new common serveice: %v", err)
	}
	if oracleContractAddress == "" {
		return nil, nil, nil, nil, fmt.Errorf("oracle contract address is empty")
	}
	oracleServer, err := oracle.NewServer(logger, backend, oracleContractAddress, signer, cc, subPub)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("new oracle service: %w", err)
	}

	address, err := signer.EthereumAddress()
	logger.Infof("address  %s", address.String())
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("chain address: %w", err)
	}
	transactionService, err := transaction.NewService(logger, backend, signer, stateStore, cc, chainID)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("new transaction service: %w", err)
	}

	if !trafficEnable {
		service := pseudosettle.New(p2pService, logger, stateStore, address)
		if err = service.Init(); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("InitTraffic:: %w", err)
		}

		return oracleServer, service, service, cc, nil
	}
	trafficChainService, err := chainTraffic.NewServer(logger, chainID, backend, signer, transactionService, trafficContractAddr, cc)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("new traffic service: %w", err)
	}

	trafficService, err := InitTraffic(stateStore, address, trafficChainService, transactionService, logger, p2pService, signer, chainID.Int64(), trafficContractAddr, subPub)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	err = trafficService.Init()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("InitChain: %w", err)
	}

	return oracleServer, trafficService, trafficService, cc, nil
}

func InitTraffic(store storage.StateStorer, address common.Address, trafficChainService chain.Traffic,
	transactionService chain.Transaction, logger logging.Logger, p2pService *libp2p.Service, signer crypto.Signer, chainID int64, trafficContractAddr string, subPub subscribe.SubPub) (*traffic.Service, error) {
	chequeStore := chequePkg.NewChequeStore(store, address, chequePkg.RecoverCheque, chainID)
	cashOut := chequePkg.NewCashoutService(store, transactionService, trafficChainService, chequeStore, common.HexToAddress(trafficContractAddr))
	addressBook := traffic.NewAddressBook(store)
	protocol := trafficprotocol.New(p2pService, logger, address)
	if err := p2pService.AddProtocol(protocol.Protocol()); err != nil {
		return nil, fmt.Errorf("traffic server :%v", err)
	}
	chequeSigner := chequePkg.NewChequeSigner(signer, chainID)
	trafficService := traffic.New(logger, address, store, trafficChainService, chequeStore, cashOut, p2pService, addressBook, chequeSigner, protocol, chainID, subPub)
	protocol.SetTraffic(trafficService)
	return trafficService, nil
}
