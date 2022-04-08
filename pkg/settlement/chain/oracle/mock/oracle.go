package mock

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
)

type ChainOracle struct {
	// logger logging.Logger
	// oracle *oracle.Oracle
}

func (ora *ChainOracle) API() rpc.API {
	// TODO implement me
	panic("implement me")
}

func NewServer() *ChainOracle {
	return &ChainOracle{}
}

func (ora *ChainOracle) GetCid(aufsUri string) []byte {
	return nil
}

func (ora *ChainOracle) GetNodesFromCid(cid []byte) []boson.Address {
	overs := make([]boson.Address, 0)
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
	return common.Hash{}, nil
}
func (ora *ChainOracle) RemoveCidAndNode(ctx context.Context, rootCid boson.Address, address boson.Address) (common.Hash, error) {
	return common.Hash{}, nil
}

func (ora *ChainOracle) GetRegisterState(ctx context.Context, rootCid boson.Address, address boson.Address) (bool, error) {
	return false, nil
}

func (ora *ChainOracle) WaitForReceipt(ctx context.Context, rootCid boson.Address, txHash common.Hash) (receipt *types.Receipt, err error) {
	return nil, nil
}
