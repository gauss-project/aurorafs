package oracle

import (
	"context"
	"github.com/ethersphere/bee/pkg/bigint"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/swarm"
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

func (ora *ChainOracle) PeerPrice(address swarm.Address) uint64{
	return 1
}
func (ora *ChainOracle)  Price() uint64{
	return 1
}

func (ora *ChainOracle) SetPrice( value uint64, signature []byte) {

}

func (ora *ChainOracle)GetCid(aufsUri string) []byte {
	return nil
}

func (ora *ChainOracle)GetNodesFromCid([]byte) []swarm.Address{
	return nil
}

func (ora *ChainOracle)GetSourceNodes(aufsUri string) []swarm.Address{

	return nil
}

func (ora *ChainOracle)OnStoreMatched(cid cid.Cid,dataLen uint64, salt uint64,address swarm.Address){

}
func (ora *ChainOracle)DataStoreFinished(cid cid.Cid,dataLen uint64, salt uint64,proof []byte){

}

func (ora *ChainOracle) CheckAutoInfo([]byte) bool{
	return false
}

func (ora *ChainOracle)ReportInvalidAuthInfo([]byte){

}

func (ora *ChainOracle) GetPeerBalance(peer swarm.Address) *bigint.BigInt{

	return bigint.Wrap(big.NewInt(1000000))
}

func (ora *ChainOracle) GetSentAmount(peer swarm.Address){

}

func (ora *ChainOracle) GetReceivedAmount(peer swarm.Address){

}

func (ora *ChainOracle) ReportSignedAmount(voucher []byte, signature []byte){

}

