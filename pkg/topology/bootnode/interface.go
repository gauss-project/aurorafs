package bootnode

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
)

type BootNodes interface {
	Connected(context.Context, p2p.Peer)
	Disconnected(p2p.Peer)
	Count() int
	RandomPeer(boson.Address) (boson.Address, error)
	EachPeer(pf model.EachPeerFunc) error
}
