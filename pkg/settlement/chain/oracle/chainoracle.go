package oracle

import (
	"context"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
)

type ChainOracle struct {
	logger        logging.Logger
	oracle        *Oracle
	chain         *ethclient.Client
	signer        crypto.Signer
	senderAddress common.Address
	chainID       *big.Int
}

func NewServer(logger logging.Logger, backend *ethclient.Client, address string, signer crypto.Signer) (chain.Resolver, error) {
	senderAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	chainID, err := backend.ChainID(context.Background())
	if err != nil {
		logger.Infof("could not connect to backend  In a swap-enabled network a working blockchain node (for goerli network in production) is required. Check your node or specify another node using --traffic-endpoint.")
		return nil, err
	}
	oracle, err := NewOracle(common.HexToAddress(address), backend)
	if err != nil {
		logger.Errorf("Failed to connect to the Ethereum client: %v", err)
		return &ChainOracle{}, err
	}

	return &ChainOracle{
		logger:        logger,
		oracle:        oracle,
		chain:         backend,
		signer:        signer,
		senderAddress: senderAddress,
		chainID:       chainID,
	}, nil
}

func (ora *ChainOracle) GetCid(aufsUri string) []byte {
	return nil
}

func (ora *ChainOracle) GetNodesFromCid(cid []byte) []boson.Address {
	overlays, err := ora.oracle.Get(nil, common.BytesToHash(cid))
	overs := make([]boson.Address, 0)
	if err != nil {
		ora.logger.Errorf("Get overlays based on cid : %v", err)
		return overs
	}
	for i := range overlays {
		overs = append(overs, boson.NewAddress(overlays[i][:]))
	}
	return overs
}

func (ora *ChainOracle) GetSourceNodes(aufsUri string) []boson.Address {

	return nil
}

func (ora *ChainOracle) OnStoreMatched(cid boson.Address, dataLen uint64, salt uint64, address boson.Address) {

}

func (ora *ChainOracle) DataStoreFinished(cid boson.Address, dataLen uint64, salt uint64, proof []byte, resCh chan chain.ChainResult) {

}

func (ora *ChainOracle) RegisterCidAndNode(ctx context.Context, rootCid boson.Address, address boson.Address) (common.Hash, error) {
	opts, err := ora.getTransactOpts(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	tract, err := ora.oracle.Set(opts, common.BytesToHash(rootCid.Bytes()), common.BytesToHash(address.Bytes()))
	if err != nil {
		return common.Hash{}, err
	}

	return tract.Hash(), nil
}
func (ora *ChainOracle) RemoveCidAndNode(ctx context.Context, rootCid boson.Address, address boson.Address) (common.Hash, error) {
	opts, err := ora.getTransactOpts(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	tract, err := ora.oracle.Remove(opts, common.BytesToHash(rootCid.Bytes()), common.BytesToHash(address.Bytes()))
	if err != nil {
		return common.Hash{}, err
	}
	return tract.Hash(), nil
}

func (ora *ChainOracle) GetRegisterState(ctx context.Context, rootCid boson.Address, address boson.Address) (bool, error) {

	state, err := ora.oracle.OracleIMap(nil, common.BytesToHash(rootCid.Bytes()), common.BytesToHash(address.Bytes()))
	if err != nil {
		return false, err
	}

	return state.Cmp(big.NewInt(0)) != 0, nil
}

func (ora *ChainOracle) getTransactOpts(ctx context.Context) (*bind.TransactOpts, error) {
	chainNonce, err := ora.chain.PendingNonceAt(ctx, ora.senderAddress)
	if err != nil {
		return nil, err
	}

	gasPrice, err := ora.chain.SuggestGasPrice(ctx)
	if err != nil {
		return nil, err
	}

	opts := &bind.TransactOpts{
		From: ora.senderAddress,
		Signer: func(address common.Address, tx *types.Transaction) (*types.Transaction, error) {
			if address != ora.senderAddress {
				return nil, bind.ErrNotAuthorized
			}
			return ora.signer.SignTx(tx, ora.chainID)
		},
		GasLimit: 1000000,
		GasPrice: gasPrice,
		Context:  ctx,
		Nonce:    new(big.Int).SetUint64(chainNonce),
	}
	return opts, nil
}
