package node

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap/chequebook"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap/oracle"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap/swapprotocol"
	"github.com/gauss-project/aurorafs/pkg/storage"
)


// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transacton Service to interact with it using the provided signer.
func InitChain(
	ctx context.Context,
	logger logging.Logger,
	endpoint string,
	contractAddress string,
) (*ethclient.Client, *oracle.ChainOracle, error) {

	backend, err := ethclient.Dial(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("dial eth client: %w", err)
	}

	_, err = backend.ChainID(ctx)
	if err != nil {
		logger.Infof("could not connect to backend at %v. In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --swap-endpoint.", endpoint)
		return nil, nil, fmt.Errorf("get chain id: %w", err)
	}
	if contractAddress == "" {
		return nil, nil, fmt.Errorf("oracle contract address is empty")
	}
	oracleServer, err := oracle.NewServer(logger, backend, contractAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("new oracle service: %w", err)
	}
	return backend, oracleServer, nil
}


// InitSwap will initialize and register the swap service.
func InitSwap(
	p2ps *libp2p.Service,
	logger logging.Logger,
	stateStore storage.StateStorer,
	networkID uint64,
	overlayEthAddress common.Address,
	chequebookService chequebook.Service,
	chequeStore chequebook.ChequeStore,
	cashoutService chequebook.CashoutService,
) (*swap.Service, error) {
	swapProtocol := swapprotocol.New(p2ps, logger, overlayEthAddress)
	swapAddressBook := swap.NewAddressbook(stateStore)

	swapService := swap.New(
		swapProtocol,
		logger,
		stateStore,
		chequebookService,
		chequeStore,
		swapAddressBook,
		networkID,
		cashoutService,
		p2ps,
	)

	swapProtocol.SetSwap(swapService)

	err := p2ps.AddProtocol(swapProtocol.Protocol())
	if err != nil {
		return nil, err
	}

	return swapService, nil
}
