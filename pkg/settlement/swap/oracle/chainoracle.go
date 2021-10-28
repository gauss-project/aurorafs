package oracle

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"math/big"
)

type ChainOracle struct {
	logger logging.Logger
	oracle *Oracle
}

func NewServer(logger logging.Logger, backend *ethclient.Client, address string) (*ChainOracle, error) {

	oracle, err := NewOracle(common.HexToAddress(address), backend)
	if err != nil {
		logger.Errorf("Failed to connect to the Ethereum client: %v", err)
		return &ChainOracle{}, err
	}
	return &ChainOracle{
		logger: logger,
		oracle: oracle,
	}, nil
}

func (ora *ChainOracle) PeerPrice(address boson.Address) uint64 {
	return 1
}
func (ora *ChainOracle) Price() uint64 {
	return 1
}

func (ora *ChainOracle) SetPrice(value uint64, signature []byte, resCh chan ChainResult) {

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

func (ora *ChainOracle) DataStoreFinished(cid boson.Address, dataLen uint64, salt uint64, proof []byte, resCh chan ChainResult) {

}

func (ora *ChainOracle) CheckAutoInfo([]byte) bool {
	return false
}

func (ora *ChainOracle) ReportInvalidAuthInfo([]byte, chan ChainResult) {

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

func (ora *ChainOracle) ReportSignedAmount(voucher []byte, signature []byte, resCh chan ChainResult) {

}
