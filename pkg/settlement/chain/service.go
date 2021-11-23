package chain

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"math/big"
)

type ChainResult struct {
	//success bool
	TxHash []byte
	//reason  string
}
type Price interface {
	// PeerPrice Get price for specified node
	PeerPrice(address boson.Address) uint64
	// Price Get common price from chain
	Price() uint64
	// SetPrice set price for this peer
	SetPrice(value uint64, signature []byte, resCh chan ChainResult)
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

type AuthInfo interface {
	// CheckAutoInfo Check validate of an auth_info from chain
	CheckAutoInfo([]byte) bool
	// ReportInvalidAuthInfo Report invalid auth_info to chain
	ReportInvalidAuthInfo([]byte, chan ChainResult)
}

type Balance interface {
	// GetPeerBalance Get the balance of peer account
	GetPeerBalance(peer boson.Address) *big.Int
}

type Cheque interface {
	// GetSentAmount Get data traffic amount from this peer to target peer
	GetSentAmount(peer boson.Address) *big.Int
	// GetReceivedAmount Get data traffic amount from target to this peer
	GetReceivedAmount(peer boson.Address) *big.Int
	// ReportSignedAmount voucher is singed by counter-part peer, signature including my addr
	ReportSignedAmount(voucher []byte, signature []byte, resCh chan ChainResult)
}
