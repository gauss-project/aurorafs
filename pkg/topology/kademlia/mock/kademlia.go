package mock

import (
	"context"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/topology/model"
)

type AddrTuple struct {
	Addr boson.Address // the peer address
	PO   uint8         // the po
}

func WithEachPeerRevCalls(addrs ...AddrTuple) Option {
	return optionFunc(func(m *Mock) {
		for _, a := range addrs {
			a := a
			m.eachPeerRev = append(m.eachPeerRev, a)
		}
	})
}

func WithDepth(d uint8) Option {
	return optionFunc(func(m *Mock) {
		m.depth = d
	})
}

func WithDepthCalls(d ...uint8) Option {
	return optionFunc(func(m *Mock) {
		m.depthReplies = d
	})
}

type Mock struct {
	mtx          sync.Mutex
	peers        []boson.Address
	eachPeerRev  []AddrTuple
	depth        uint8
	depthReplies []uint8
	depthCalls   int
	subPub       subscribe.SubPub
}

func (m *Mock) RefreshProtectPeer(peer []boson.Address) {
	// TODO implement me
	panic("implement me")
}

func NewMockKademlia(o ...Option) *Mock {
	m := &Mock{}
	for _, v := range o {
		v.apply(m)
	}
	m.subPub = subscribe.NewSubPub()
	return m
}

// AddPeers is called when a peers are added to the topology backlog
// for further processing by connectivity strategy.
func (m *Mock) AddPeers(addr ...boson.Address) {
	panic("not implemented") // TODO: Implement
}

func (m *Mock) ClosestPeer(addr boson.Address, _ bool, _ topology.Filter, skipPeers ...boson.Address) (peerAddr boson.Address, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *Mock) IsWithinDepth(adr boson.Address) bool {
	panic("not implemented") // TODO: Implement
}

func (m *Mock) EachNeighbor(model.EachPeerFunc) error {
	panic("not implemented") // TODO: Implement
}

func (m *Mock) EachNeighborRev(model.EachPeerFunc) error {
	panic("not implemented") // TODO: Implement
}

func (m *Mock) ClosestPeers(addr boson.Address, limit int, _ topology.Filter, skipPeers ...boson.Address) ([]boson.Address, error) {
	panic("implement me")
}

func (m *Mock) DisconnectForce(addr boson.Address, reason string) error {
	m.Disconnected(p2p.Peer{
		Address: addr,
		Mode:    aurora.NewModel(),
	}, reason)
	return nil
}

func (m *Mock) Outbound(peer p2p.Peer) {

}

func (m *Mock) EachKnownPeer(peerFunc model.EachPeerFunc) error {
	panic("implement me")
}

func (m *Mock) EachKnownPeerRev(peerFunc model.EachPeerFunc) error {
	panic("implement me")
}

func (m *Mock) SnapshotConnected() (connected int, peers map[string]*model.PeerInfo) {
	return
}

func (m *Mock) SnapshotAddr(addr boson.Address) *model.Snapshot {
	// TODO implement me
	panic("implement me")
}

func (m *Mock) RecordPeerLatency(add boson.Address, t time.Duration) {

}

// EachPeer iterates from closest bin to farthest
func (m *Mock) EachPeer(f model.EachPeerFunc, _ topology.Filter) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for i := len(m.peers) - 1; i > 0; i-- {
		stop, _, err := f(m.peers[i], uint8(i))
		if stop {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// EachPeerRev iterates from farthest bin to closest
func (m *Mock) EachPeerRev(f model.EachPeerFunc, _ topology.Filter) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, v := range m.eachPeerRev {
		stop, _, err := f(v.Addr, v.PO)
		if stop {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Mock) NeighborhoodDepth() uint8 {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.depthCalls++
	if len(m.depthReplies) > 0 {
		return m.depthReplies[m.depthCalls]
	}
	return m.depth
}

// Connected is called when a peer dials in.
func (m *Mock) Connected(_ context.Context, peer p2p.Peer, _ bool) error {
	m.mtx.Lock()
	m.peers = append(m.peers, peer.Address)
	m.mtx.Unlock()
	m.Trigger()
	return nil
}

// Disconnected is called when a peer disconnects.
func (m *Mock) Disconnected(peer p2p.Peer, reason string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for i, addr := range m.peers {
		if addr.Equal(peer.Address) {
			m.peers = append(m.peers[:i], m.peers[i+1:]...)
			break
		}
	}
	m.Trigger()
}

func (m *Mock) Announce(_ context.Context, _ boson.Address, _ bool) error {
	return nil
}

func (m *Mock) NotifyPeerState(peer p2p.PeerInfo) {

}

func (m *Mock) AnnounceTo(_ context.Context, _, _ boson.Address, _ bool) error {
	return nil
}

func (m *Mock) SubscribePeersChange(notifier subscribe.INotifier) {
	_ = m.subPub.Subscribe(notifier, "kad", "peerChange", "")
}

func (m *Mock) SubscribePeerState(notifier subscribe.INotifier) {
	_ = m.subPub.Subscribe(notifier, "kad", "peerState", "")
}

func (m *Mock) Trigger() {
	_ = m.subPub.Publish("kad", "peerChange", "", m.peers)
}

func (m *Mock) ResetPeers() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.peers = nil
	m.eachPeerRev = nil
}

func (m *Mock) Halt()        {}
func (m *Mock) Close() error { return nil }

func (m *Mock) Snapshot() *model.KadParams {
	panic("not implemented") // TODO: Implement
}

type Option interface {
	apply(*Mock)
}
type optionFunc func(*Mock)

func (f optionFunc) apply(r *Mock) { f(r) }
