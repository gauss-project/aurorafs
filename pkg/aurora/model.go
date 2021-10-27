package aurora

import "github.com/gauss-project/aurorafs/pkg/bitvector"

const (
	FullNode = iota
	LightNode
	BootNode
)

type Model struct {
	Bv *bitvector.BitVector
}

func (m Model) IsFull() bool {
	return m.Bv.Get(FullNode)
}

func (m Model) IsLight() bool {
	return m.Bv.Get(LightNode)
}

func (m Model) IsBootNode() bool {
	return m.Bv.Get(BootNode)
}

// AddressInfo contains the information received from the handshake.
type AddressInfo struct {
	Address  *Address
	NodeMode Model
}

func (i *AddressInfo) LightString() string {
	if i.NodeMode.IsBootNode() {
		return " (boot-node)"
	}
	if i.NodeMode.IsLight() {
		return " (light)"
	}

	return ""
}
