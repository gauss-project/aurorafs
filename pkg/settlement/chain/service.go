package chain

import (
	"context"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
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

// Service is the service to send transactions. It takes care of gas price, gas
// limit and nonce management.
type Transaction interface {
	// Send creates a transaction based on the request and sends it.
	Send(ctx context.Context, request *transaction.TxRequest) (txHash common.Hash, err error)
	// Call simulate a transaction based on the request.
	Call(ctx context.Context, request *transaction.TxRequest) (result []byte, err error)
	// WaitForReceipt waits until either the transaction with the given hash has been mined or the context is cancelled.
	WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)
}
