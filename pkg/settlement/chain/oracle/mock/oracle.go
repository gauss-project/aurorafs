package mock

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
)

type ChainOracle struct {
	//logger logging.Logger
	//oracle *oracle.Oracle
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
