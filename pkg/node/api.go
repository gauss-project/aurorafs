package node

import (
	"github.com/gauss-project/aurorafs/pkg/rpc"
)

// apis returns the collection of built-in RPC APIs.
func (n *Node) apis() []rpc.API {
	return []rpc.API{}
}
