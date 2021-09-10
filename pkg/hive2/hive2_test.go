package hive2_test

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/discovery/mock"
	"github.com/gauss-project/aurorafs/pkg/hive2"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	p2pmock "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/shed"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"io"
	"testing"
)

const underlayBase = "/ip4/127.0.0.1/tcp/1634/dns/"

var (
	nonConnectableAddress, _        = ma.NewMultiaddr(underlayBase + "16Uiu2HAkx8ULY8cTXhdVAcMmLcH9AsTKz6uBQ7DPLKRjMLgBVYkA")
	noopLogger                      = logging.New(io.Discard, logrus.TraceLevel)
	networkId                uint64 = 0
	portArray                       = []int{16330}
)

type Node struct {
	peer   *aurora.Address
	signer crypto.Signer
	book   addressbook.Interface
	p2ps   *streamtest.Recorder
	Hive2  *hive2.Service
	kad    *kademlia.Kad
}

func newNode(t *testing.T) *Node {
	t.Helper()

	addr, kad, signer, ab := newTestKademlia(t)
	stream := streamtest.New(streamtest.WithBaseAddr(addr.Overlay))
	h := hive2.New(stream, ab, networkId, noopLogger)
	h.SetConfig(hive2.Config{Kad: kad})
	stream.SetProtocols(h.Protocol())

	return &Node{
		peer:   addr,
		signer: signer,
		book:   ab,
		p2ps:   stream,
		Hive2:  h,
		kad:    kad,
	}
}

func p2pMock(ab addressbook.Interface, overlay boson.Address, signer crypto.Signer) p2p.Service {
	p2ps := p2pmock.New(p2pmock.WithConnectFunc(func(ctx context.Context, underlay ma.Multiaddr) (*aurora.Address, error) {
		if underlay.Equal(nonConnectableAddress) {
			return nil, errors.New("non reachable node")
		}
		addresses, err := ab.Addresses()
		if err != nil {
			return nil, errors.New("could not fetch addressBook addresses")
		}

		for _, a := range addresses {
			if a.Underlay.Equal(underlay) {
				return &a, nil
			}
		}

		bzzAddr, err := aurora.NewAddress(signer, underlay, overlay, networkId)
		if err != nil {
			return nil, err
		}

		if err := ab.Put(overlay, *bzzAddr); err != nil {
			return nil, err
		}

		return bzzAddr, nil
	}))

	return p2ps
}

func newTestKademlia(t *testing.T) (*aurora.Address, *kademlia.Kad, crypto.Signer, addressbook.Interface) {
	metricsDB, err := shed.NewDB("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := metricsDB.Close(); err != nil {
			t.Fatal(err)
		}
	})

	base, signer := randomAddress(t)
	ab := addressbook.New(mockstate.NewStateStore()) // address book
	p2ps := p2pMock(ab, base.Overlay, signer)
	disc := mock.NewDiscovery()
	kad := kademlia.New(base.Overlay, ab, disc, p2ps, metricsDB, noopLogger, kademlia.Options{BinMaxPeers: 10}) // kademlia instance
	err = kad.Start(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	p2ps.SetPickyNotifier(kad)
	return base, kad, signer, ab
}

func randomAddress(t *testing.T) (addr *aurora.Address, signer crypto.Signer) {
	pk, _ := crypto.GenerateSecp256k1Key()
	signer = crypto.NewDefaultSigner(pk)
	base, _ := crypto.NewOverlayAddress(pk.PublicKey, networkId)
	mu, err := ma.NewMultiaddr(underlayBase + base.String())
	if err != nil {
		t.Fatal(err)
	}
	auroraAddr, err := aurora.NewAddress(signer, mu, base, networkId)
	if err != nil {
		t.Fatal(err)
	}
	return auroraAddr, signer
}

func (s *Node) addOne(t *testing.T, peer *aurora.Address, connect bool) {
	t.Helper()

	if err := s.book.Put(peer.Overlay, *peer); err != nil {
		t.Fatal(err)
	}
	if connect {
		err := s.kad.Connected(context.TODO(), p2p.Peer{Address: peer.Overlay}, true)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestService_DoFindNode(t *testing.T) {

	nodes := make([]*Node, 20)
	for i := 0; i < 20; i++ {
		nodes[i] = newNode(t)
	}

	nodes[0].addOne(t, nodes[1].peer, true)
	nodes[1].addOne(t, nodes[0].peer, true)

	for i := 2; i < 20; i++ {
		nodes[1].addOne(t, nodes[i].peer, true)
	}
	s1 := nodes[1].kad.Snapshot()
	if s1.Connected != 19 {
		t.Fatalf("connected expected 19 got %d", s1.Connected)
	}
	s0 := nodes[0].kad.Snapshot()
	if s0.Connected != 1 {
		t.Fatalf("connected expected 1 got %d", s0.Connected)
	}
	nodes[0].Hive2.DiscoverWork()
	s01 := nodes[0].kad.Snapshot()
	if s0.Connected != 17 {
		t.Fatalf("connected expected 17 got %d", s01.Connected)
	}

}
