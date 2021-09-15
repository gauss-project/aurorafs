package lightnode

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
)

type Container struct {
	base              boson.Address
	peerMu            sync.Mutex // peerMu guards connectedPeers and disconnectedPeers.
	connectedPeers    *pslice.PSlice
	disconnectedPeers *pslice.PSlice
	metrics           metrics
}

func NewContainer(base boson.Address) *Container {
	return &Container{
		base:              base,
		connectedPeers:    pslice.New(1, base),
		disconnectedPeers: pslice.New(1, base),
		metrics:           newMetrics(),
	}
}

func (c *Container) Connected(ctx context.Context, peer p2p.Peer) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()

	addr := peer.Address
	c.connectedPeers.Add(addr)
	c.disconnectedPeers.Remove(addr)

	c.metrics.CurrentlyConnectedPeers.Set(float64(c.connectedPeers.Length()))
	c.metrics.CurrentlyDisconnectedPeers.Set(float64(c.disconnectedPeers.Length()))
}

func (c *Container) Disconnected(peer p2p.Peer) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()

	addr := peer.Address
	if found := c.connectedPeers.Exists(addr); found {
		c.connectedPeers.Remove(addr)
		c.disconnectedPeers.Add(addr)
	}

	c.metrics.CurrentlyConnectedPeers.Set(float64(c.connectedPeers.Length()))
	c.metrics.CurrentlyDisconnectedPeers.Set(float64(c.disconnectedPeers.Length()))
}

func (c *Container) Count() int {
	return c.connectedPeers.Length()
}

func (c *Container) RandomPeer(not boson.Address) (boson.Address, error) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	var (
		cnt   = big.NewInt(int64(c.Count()))
		addr  = boson.ZeroAddress
		count = int64(0)
	)

PICKPEER:
	i, e := rand.Int(rand.Reader, cnt)
	if e != nil {
		return boson.ZeroAddress, e
	}
	i64 := i.Int64()

	count = 0
	_ = c.connectedPeers.EachBinRev(func(peer boson.Address, _ uint8) (bool, bool, error) {
		if count == i64 {
			addr = peer
			return true, false, nil
		}
		count++
		return false, false, nil
	})

	if addr.Equal(not) {
		goto PICKPEER
	}

	return addr, nil
}

func (c *Container) EachPeer(pf topology.EachPeerFunc) error {
	return c.connectedPeers.EachBin(pf)
}

func (c *Container) PeerInfo() topology.BinInfo {
	return topology.BinInfo{
		BinPopulation:     uint(c.connectedPeers.Length()),
		BinConnected:      uint(c.connectedPeers.Length()),
		DisconnectedPeers: peersInfo(c.disconnectedPeers),
		ConnectedPeers:    peersInfo(c.connectedPeers),
	}
}

func peersInfo(s *pslice.PSlice) []*topology.PeerInfo {
	if s.Length() == 0 {
		return nil
	}
	peers := make([]*topology.PeerInfo, 0, s.Length())
	_ = s.EachBin(func(addr boson.Address, po uint8) (bool, bool, error) {
		peers = append(peers, &topology.PeerInfo{Address: addr})
		return false, false, nil
	})
	return peers
}
