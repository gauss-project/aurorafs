package mock

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/swap/oracle"
	"math/big"
)

type ChainOracle struct {
	//logger logging.Logger
	//oracle *oracle.Oracle
}

func NewServer() *ChainOracle {
	return &ChainOracle{}
}

func (ora *ChainOracle) PeerPrice(address boson.Address) uint64 {
	return 1
}
func (ora *ChainOracle) Price() uint64 {
	return 1
}

func (ora *ChainOracle) SetPrice(value uint64, signature []byte, resCh chan oracle.ChainResult) {

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

func (ora *ChainOracle) DataStoreFinished(cid boson.Address, dataLen uint64, salt uint64, proof []byte, resCh chan oracle.ChainResult) {

}

func (ora *ChainOracle) CheckAutoInfo([]byte) bool {
	return false
}

func (ora *ChainOracle) ReportInvalidAuthInfo([]byte, chan oracle.ChainResult) {

}

func (ora *ChainOracle) GetPeerBalance(peer boson.Address) *big.Int {

	return big.NewInt(1000000)
}

func (ora *ChainOracle) GetSentAmount(peer boson.Address) *big.Int {
	return big.NewInt(0)
}

func (ora *ChainOracle) GetReceivedAmount(peer boson.Address) *big.Int {
	return big.NewInt(0)
}

func (ora *ChainOracle) ReportSignedAmount(voucher []byte, signature []byte, resCh chan oracle.ChainResult) {

}
