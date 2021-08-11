package oracle

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/ipfs/go-cid"
	"math/big"
	"time"
)

type ChainOracle struct {
	p2pCtx  context.Context
	logger logging.Logger
	pollingInterval time.Duration
	endpoint string
	networkID uint64
}
func Init(p2pCtx  context.Context,logger logging.Logger,endpoint string,pollingInterval time.Duration,networkID uint64) (ChainOracle,error)  {

	return ChainOracle{
		p2pCtx: p2pCtx,
		logger: logger,
		endpoint: endpoint,
		pollingInterval: pollingInterval,
		networkID: networkID,
	},nil
}

func (ora *ChainOracle) PeerPrice(address boson.Address) uint64{
	return 1
}
func (ora *ChainOracle)  Price() uint64{
	return 1
}

func (ora *ChainOracle) SetPrice( value uint64, signature []byte,resCh chan ChainResult) {

}

func (ora *ChainOracle)GetCid(aufsUri string) []byte {
	return nil
}

func (ora *ChainOracle)GetNodesFromCid([]byte) []boson.Address{
	return nil
}

func (ora *ChainOracle)GetSourceNodes(aufsUri string) []boson.Address{

	return nil
}

func (ora *ChainOracle)OnStoreMatched(cid cid.Cid,dataLen uint64, salt uint64,address boson.Address){

}

func (ora *ChainOracle)DataStoreFinished(cid cid.Cid,dataLen uint64, salt uint64,proof []byte,resCh chan ChainResult){

}

func (ora *ChainOracle) CheckAutoInfo([]byte) bool{
	return false
}

func (ora *ChainOracle)ReportInvalidAuthInfo([]byte,chan ChainResult){

}

func (ora *ChainOracle) GetPeerBalance(peer boson.Address) *big.Int{

	return big.NewInt(1000000)
}

func (ora *ChainOracle) GetSentAmount(peer boson.Address) *big.Int{
	return big.NewInt(0)
}

func (ora *ChainOracle) GetReceivedAmount(peer boson.Address) *big.Int{
	return big.NewInt(0)
}

func (ora *ChainOracle) ReportSignedAmount(voucher []byte, signature []byte,resCh chan ChainResult){

}

