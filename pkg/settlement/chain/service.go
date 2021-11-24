package chain

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"math/big"
)

type ChainResult struct {
	//success bool
	TxHash []byte
	//reason  string
}

type Resolver interface {
	// GetCid Resolve cid from  uri
	GetCid(aufsUri string) []byte

	// GetNodesFromCid  Get source nodes of specified cid
	GetNodesFromCid([]byte) []boson.Address

	// GetSourceNodes  Short hand function, get storage nodes from uri
	GetSourceNodes(aufsUri string) []boson.Address

	// OnStoreMatched Notification when new data req matched
	OnStoreMatched(cid boson.Address, dataLen uint64, salt uint64, address boson.Address)

	// DataStoreFinished when data retrieved and saved, use this function to report onchain
	DataStoreFinished(cid boson.Address, dataLen uint64, salt uint64, proof []byte, resCh chan ChainResult)
}

type Traffic interface {

	// 	TransferredAddress opts todo
	TransferredAddress(opts *bind.CallOpts, address common.Address, arg1 *big.Int) (common.Address, error)

	RetrievedAddress(opts *bind.CallOpts, address common.Address, arg1 *big.Int) (common.Address, error)

	BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error)

	RetrievedTotal(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error)
}
