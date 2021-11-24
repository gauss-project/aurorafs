package chain

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
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
}
