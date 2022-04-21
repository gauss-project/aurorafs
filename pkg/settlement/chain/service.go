package chain

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"math/big"
)

type ChainResult struct {
	//success bool
	TxHash []byte
	//reason  string
}

// TxRequest describes a request for a transaction that can be executed.
type TxRequest struct {
	To       *common.Address // recipient of the transaction
	Data     []byte          // transaction data
	GasPrice *big.Int        // gas price or nil if suggested gas price should be used
	GasLimit uint64          // gas limit or 0 if it should be estimated
	Value    *big.Int        // amount of wei to send
}

type AllRequest struct {
	Method string
	Params []interface{}
}

type AllResponse struct {
	Result interface{}
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
	RegisterCidAndNode(ctx context.Context, rootCid boson.Address, address boson.Address) (common.Hash, error)
	RemoveCidAndNode(ctx context.Context, rootCid boson.Address, address boson.Address) (common.Hash, error)
	GetRegisterState(ctx context.Context, rootCid boson.Address, address boson.Address) (bool, error)
	WaitForReceipt(ctx context.Context, rootCid boson.Address, txHash common.Hash) (receipt *types.Receipt, err error)
	API() rpc.API
}

type Traffic interface {

	// 	TransferredAddress opts todo
	TransferredAddress(address common.Address) ([]common.Address, error)

	RetrievedAddress(address common.Address) ([]common.Address, error)

	BalanceOf(account common.Address) (*big.Int, error)

	RetrievedTotal(address common.Address) (*big.Int, error)

	TransferredTotal(address common.Address) (*big.Int, error)

	TransAmount(beneficiary, recipient common.Address) (*big.Int, error)

	CashChequeBeneficiary(ctx context.Context, beneficiary, recipient common.Address, cumulativePayout *big.Int, signature []byte) (*types.Transaction, error)
}

// Service is the service to send transactions. It takes care of gas price, gas
// limit and nonce management.
type Transaction interface {
	// Send creates a transaction based on the request and sends it.
	Send(ctx context.Context, request *TxRequest) (txHash common.Hash, err error)
	// Call simulate a transaction based on the request.
	Call(ctx context.Context, request *TxRequest) (result []byte, err error)
	// WaitForReceipt waits until either the transaction with the given hash has been mined or the context is cancelled.
	WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)

	PutNonce(nonce uint64) error

	NextNonce(ctx context.Context) (uint64, error)
}

type Common interface {
	All(ctx context.Context, request *AllRequest) (*AllResponse, error)
}
