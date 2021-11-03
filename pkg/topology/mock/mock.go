package mock

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/topology"
)

type mock struct {
	peers           []boson.Address
	depth           uint8
	closestPeer     boson.Address
	closestPeerErr  error
	peersErr        error
	addPeersErr     error
	isWithinFunc    func(c boson.Address) bool
	marshalJSONFunc func() ([]byte, error)
	mtx             sync.Mutex
}

func WithPeers(peers ...boson.Address) Option {
	return optionFunc(func(d *mock) {
		d.peers = peers
	})
}

func WithAddPeersErr(err error) Option {
	return optionFunc(func(d *mock) {
		d.addPeersErr = err
	})
}

func WithNeighborhoodDepth(dd uint8) Option {
	return optionFunc(func(d *mock) {
		d.depth = dd
	})
}

func WithClosestPeer(addr boson.Address) Option {
	return optionFunc(func(d *mock) {
		d.closestPeer = addr
	})
}

func WithClosestPeerErr(err error) Option {
	return optionFunc(func(d *mock) {
		d.closestPeerErr = err
	})
}

func WithMarshalJSONFunc(f func() ([]byte, error)) Option {
	return optionFunc(func(d *mock) {
		d.marshalJSONFunc = f
	})
}

func WithIsWithinFunc(f func(boson.Address) bool) Option {
	return optionFunc(func(d *mock) {
		d.isWithinFunc = f
	})
}

func NewTopologyDriver(opts ...Option) topology.Driver {
	d := new(mock)
	for _, o := range opts {
		o.apply(d)
	}
	return d
}

func (d *mock) AddPeers(addrs ...boson.Address) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.peers = append(d.peers, addrs...)
}

func (d *mock) Connected(ctx context.Context, peer p2p.Peer, _ bool) error {
	d.AddPeers(peer.Address)
	return nil
}
func (d *mock) Outbound(peer p2p.Peer) {
	d.AddPeers(peer.Address)
}

func (d *mock) DisconnectForce(addr boson.Address, reason string) error {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	for i, p := range d.peers {
		if p.Equal(addr) {
			d.peers = append(d.peers[:i], d.peers[i+1:]...)
			return nil
		}
	}
	return p2p.ErrPeerNotFound
}

func (d *mock) Disconnected(peer p2p.Peer) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	for i, addr := range d.peers {
		if addr.Equal(peer.Address) {
			d.peers = append(d.peers[:i], d.peers[i+1:]...)
			break
		}
	}
}

func (d *mock) Announce(_ context.Context, _ boson.Address, _ bool) error {
	return nil
}

func (d *mock) AnnounceTo(_ context.Context, _, _ boson.Address, _ bool) error {
	return nil
}

func (d *mock) Peers() []boson.Address {
	return d.peers
}

func (d *mock) EachKnownPeer(f model.EachPeerFunc) error {
	return nil
}

func (d *mock) EachKnownPeerRev(f model.EachPeerFunc) error {
	return nil
}

func (d *mock) ClosestPeer(addr boson.Address, wantSelf bool, skipPeers ...boson.Address) (peerAddr boson.Address, err error) {
	if len(skipPeers) == 0 {
		if d.closestPeerErr != nil {
			return d.closestPeer, d.closestPeerErr
		}
		if !d.closestPeer.Equal(boson.ZeroAddress) {
			return d.closestPeer, nil
		}
	}

	d.mtx.Lock()
	defer d.mtx.Unlock()

	if len(d.peers) == 0 {
		return peerAddr, topology.ErrNotFound
	}

	skipPeer := false
	for _, p := range d.peers {
		for _, a := range skipPeers {
			if a.Equal(p) {
				skipPeer = true
				break
			}
		}
		if skipPeer {
			skipPeer = false
			continue
		}

		if peerAddr.IsZero() {
			peerAddr = p
		}

		if cmp, _ := boson.DistanceCmp(addr.Bytes(), p.Bytes(), peerAddr.Bytes()); cmp == 1 {
			peerAddr = p
		}
	}

	if peerAddr.IsZero() {
		if wantSelf {
			return peerAddr, topology.ErrWantSelf
		} else {
			return peerAddr, topology.ErrNotFound
		}
	}

	return peerAddr, nil
}

func (d *mock) ClosestPeers(addr boson.Address, limit int, skipPeers ...boson.Address) ([]boson.Address, error) {
	return nil, nil
}

func (d *mock) SubscribePeersChange() (c <-chan struct{}, unsubscribe func()) {
	return c, unsubscribe
}

func (m *mock) NeighborhoodDepth() uint8 {
	return m.depth
}

func (m *mock) IsWithinDepth(addr boson.Address) bool {
	if m.isWithinFunc != nil {
		return m.isWithinFunc(addr)
	}
	return false
}

func (m *mock) EachNeighbor(f model.EachPeerFunc) error {
	return m.EachPeer(f)
}

func (*mock) EachNeighborRev(model.EachPeerFunc) error {
	panic("not implemented") // TODO: Implement
}

// EachPeer iterates from closest bin to farthest
func (d *mock) EachPeer(f model.EachPeerFunc) (err error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if d.peersErr != nil {
		return d.peersErr
	}

	for i, p := range d.peers {
		_, _, err = f(p, uint8(i))
		if err != nil {
			return
		}
	}

	return nil
}

// EachPeerRev iterates from farthest bin to closest
func (d *mock) EachPeerRev(f model.EachPeerFunc) (err error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	for i := len(d.peers) - 1; i >= 0; i-- {
		_, _, err = f(d.peers[i], uint8(i))
		if err != nil {
			return
		}
	}

	return nil
}

func (d *mock) Snapshot() *model.KadParams {
	return new(model.KadParams)
}

func (d *mock) Halt()        {}
func (d *mock) Close() error { return nil }

type Option interface {
	apply(*mock)
}

type optionFunc func(*mock)

func (f optionFunc) apply(r *mock) { f(r) }
