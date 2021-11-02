package routetab_test

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/discovery/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	p2pmock "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const underlayBase = "/ip4/127.0.0.1/tcp/1634/dns/"

var (
	nonConnectableAddress, _        = ma.NewMultiaddr(underlayBase + "16Uiu2HAkx8ULY8cTXhdVAcMmLcH9AsTKz6uBQ7DPLKRjMLgBVYkA")
	noopLogger                      = logging.New(io.Discard, logrus.TraceLevel)
	networkId                uint64 = 0
)

type Node struct {
	*routetab.Service
	overlay boson.Address
	addr    *aurora.Address
	peer    p2p.Peer
	signer  crypto.Signer
	book    addressbook.Interface
	p2ps    p2p.Service
	kad     *kademlia.Kad
	stream  *streamtest.Recorder
}

func p2pMock(ab addressbook.Interface, overlay boson.Address, signer crypto.Signer) p2p.Service {
	p2ps := p2pmock.New(
		p2pmock.WithConnectFunc(func(ctx context.Context, underlay ma.Multiaddr) (*aurora.Address, error) {
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
		}),
		p2pmock.WithDisconnectFunc(func(boson.Address, string) error {
			return nil
		}),
	)

	return p2ps
}

func newTestNode(t *testing.T) *Node {
	t.Helper()

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
	kad, err := kademlia.New(base.Overlay, ab, disc, p2ps, metricsDB, noopLogger, kademlia.Options{BinMaxPeers: 10}) // kademlia instance
	if err != nil {
		t.Fatal(err)
	}
	p2ps.SetPickyNotifier(kad)

	stream := streamtest.New(streamtest.WithBaseAddr(base.Overlay))
	service := routetab.New(base.Overlay, context.Background(), p2ps, kad, mockstate.NewStateStore(), noopLogger)
	service.SetConfig(routetab.Config{
		AddressBook: ab,
		NetworkID:   networkId,
		LightNodes:  lightnode.NewContainer(base.Overlay),
		Stream:      stream,
	})

	return &Node{
		overlay: base.Overlay,
		addr:    base,
		peer:    p2p.Peer{Address: base.Overlay},
		Service: service,
		signer:  signer,
		book:    ab,
		p2ps:    p2ps,
		kad:     kad,
		stream:  stream,
	}
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

func TestService_FindUnderlay(t *testing.T) {
	ctx := context.Background()

	// connect 0--1--2--3--4--5
	nodes := createTopology(t, 6)

	// find dest 4
	_, err := nodes[0].GetRoute(ctx, nodes[4].overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect dest 4 route not found")
	}
	route, err := nodes[0].FindRoute(ctx, nodes[4].overlay)
	if err != nil {
		t.Fatal(err)
	}
	if len(route) != 1 {
		t.Fatalf("expect dest 4 route path len 1,got %d", len(route))
	}
	t.Run("find route", func(t *testing.T) {
		// check route
		for i := 0; i < 3; i++ {
			_, err = nodes[0].GetRoute(ctx, nodes[4].overlay)
			if err != nil {
				t.Fatalf("i=%d %s", i, err)
			}
		}
		for i := 0; i < 2; i++ {
			_, err = nodes[i].GetRoute(ctx, nodes[3].overlay)
			if err != nil {
				t.Fatal(err)
			}
		}
		_, err = nodes[0].GetRoute(ctx, nodes[2].overlay)
		if err != nil {
			t.Fatal(err)
		}
		_, err = nodes[0].GetRoute(ctx, nodes[1].overlay)
		if !errors.Is(err, routetab.ErrNotFound) {
			t.Fatalf("node 0 expect dest 1 not found")
		}
		_, err = nodes[1].GetRoute(ctx, nodes[2].overlay)
		if !errors.Is(err, routetab.ErrNotFound) {
			t.Fatalf("node 1 expect dest 2 not found")
		}
	})

	t.Run("find underlay", func(t *testing.T) {
		// check find underlay
		_, err = nodes[0].book.Get(nodes[3].overlay)
		if !errors.Is(err, addressbook.ErrNotFound) {
			t.Fatalf("node 0 expect dest 3 underlay not found")
		}
		if nodes[0].IsNeighbor(nodes[3].overlay) {
			t.Fatalf("node 0 expect dest 3 not neighbor")
		}
		err = nodes[0].Connect(ctx, nodes[3].overlay)
		if err != nil {
			t.Fatal(err)
		}
		addr, err := nodes[0].book.Get(nodes[3].overlay)
		if err != nil {
			t.Fatal(err)
		}
		if !nodes[3].addr.Equal(addr) {
			t.Fatalf("expect address equal")
		}
	})

}

func TestService_FindRouteLoopBack(t *testing.T) {
	ctx := context.Background()

	// connect 0--1--2--3--4
	//          \______/
	nodes := createTopology(t, 5)

	nodes[0].addOne(t, nodes[3].addr, true)
	nodes[3].addOne(t, nodes[0].addr, true)

	nodes[0].SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(nodes[0].overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			nodes[1].overlay.String(): nodes[1].Protocol(),
			nodes[3].overlay.String(): nodes[3].Protocol(),
		}),
	))

	nodes[3].SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(nodes[3].overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			nodes[0].overlay.String(): nodes[0].Protocol(),
			nodes[2].overlay.String(): nodes[2].Protocol(),
			nodes[4].overlay.String(): nodes[4].Protocol(),
		}),
	))

	if nodes[0].kad.Snapshot().Connected != 2 {
		t.Fatalf("expect node 0 connected 2")
	}
	if nodes[3].kad.Snapshot().Connected != 3 {
		t.Fatalf("expect node 0 connected 3")
	}

	routetab.DefaultNeighborAlpha = 4
	routetab.MaxTTL = 4
	// find dest 4
	_, err := nodes[0].GetRoute(ctx, nodes[4].overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect dest 4 route not found")
	}
	route, err := nodes[0].FindRoute(ctx, nodes[4].overlay)
	if err != nil {
		t.Fatal(err)
	}
	if len(route) != 1 {
		t.Fatalf("expect dest 4 route path len 1,got %d", len(route))
	}
	// check route
	_, err = nodes[0].GetRoute(ctx, nodes[4].overlay)
	if err != nil {
		t.Fatal(err)
	}
}

func TestService_FindRouteMaxTTL(t *testing.T) {
	ctx := context.Background()

	// connect 0--1--2--3--4
	nodes := createTopology(t, 5)

	routetab.MaxTTL = 3
	// find dest 4
	_, err := nodes[0].GetRoute(ctx, nodes[4].overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect dest 4 route not found")
	}
	_, err = nodes[0].FindRoute(ctx, nodes[4].overlay)
	if err == nil {
		t.Fatalf("expect not find route")
	}
}

func TestService_FindRouteTargetIsNeighbor(t *testing.T) {
	ctx := context.Background()

	// connect 0--1--2--3--4
	//          \______/
	nodes := createTopology(t, 5)

	nodes[0].addOne(t, nodes[3].addr, true)
	nodes[3].addOne(t, nodes[0].addr, true)

	nodes[0].SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(nodes[0].overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			nodes[1].overlay.String(): nodes[1].Protocol(),
			nodes[3].overlay.String(): nodes[3].Protocol(),
		}),
	))

	nodes[3].SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(nodes[3].overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			nodes[0].overlay.String(): nodes[0].Protocol(),
			nodes[2].overlay.String(): nodes[2].Protocol(),
			nodes[4].overlay.String(): nodes[4].Protocol(),
		}),
	))

	if nodes[0].kad.Snapshot().Connected != 2 {
		t.Fatalf("expect node 0 connected 2")
	}
	if nodes[3].kad.Snapshot().Connected != 3 {
		t.Fatalf("expect node 0 connected 3")
	}

	// find dest 1
	_, err := nodes[0].GetRoute(ctx, nodes[1].overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect dest 1 route not found")
	}
	route, err := nodes[0].FindRoute(ctx, nodes[1].overlay)
	if err != nil {
		t.Fatal(err)
	}
	if len(route) != 1 {
		t.Fatalf("expect dest 1 route path len 1,got %d", len(route))
	}
	// check route
	r, err := nodes[0].GetRoute(ctx, nodes[1].overlay)
	if err != nil {
		t.Fatal(err)
	}
	exp := []boson.Address{nodes[0].overlay, nodes[3].overlay, nodes[2].overlay, nodes[1].overlay}
	for k, v := range r[0].Item {
		if !v.Equal(exp[k]) {
			t.Fatalf("expect item %s,got %s", exp[k], v)
		}
	}
}

func TestService_GetTargetNeighbor(t *testing.T) {
	ctx := context.Background()

	// connect 0--1--2--3--4
	nodes := createTopology(t, 5)

	routetab.MaxTTL = 5
	neighbor, err := nodes[0].GetTargetNeighbor(ctx, nodes[4].overlay, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(neighbor) != 1 {
		t.Fatalf("expect find neighbor len 1,got %d", len(neighbor))
	}
	if !neighbor[0].Equal(nodes[3].overlay) {
		t.Fatalf("expect neighbor eq node 3")
	}

	neighbor, err = nodes[0].GetTargetNeighbor(ctx, nodes[3].overlay, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(neighbor) != 2 {
		t.Fatalf("expect find neighbor len 2,got %d", len(neighbor))
	}
	for _, v := range neighbor {
		if !v.Equal(nodes[2].overlay) && !v.Equal(nodes[4].overlay) {
			t.Fatalf("expect neighbor eq node 2 or 4")
		}
	}
	// node0 offline
	err = nodes[0].kad.DisconnectForce(nodes[1].overlay, "test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodes[0].GetTargetNeighbor(ctx, nodes[3].overlay, 2)
	if err == nil {
		t.Fatalf("expect err is neighbor notfoud")
	}
}

func createTopology(t *testing.T, total int) []*Node {
	nodes := make([]*Node, 0)
	for i := 0; i < total; i++ {
		nodes = append(nodes, newTestNode(t))
		//t.Log("node", i, nodes[i].overlay)
		if i > 0 {
			nodes[i].addOne(t, nodes[i-1].addr, true)
			nodes[i-1].addOne(t, nodes[i].addr, true)
		}
	}
	for k := range nodes {
		if k == 0 {
			nodes[0].stream.SetProtocols(nodes[1].Protocol())
		} else if k == len(nodes)-1 {
			nodes[k].stream.SetProtocols(nodes[k-1].Protocol())
		} else {
			nodes[k].SetStreamer(streamtest.New(
				streamtest.WithBaseAddr(nodes[k].overlay),
				streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
					nodes[k+1].overlay.String(): nodes[k+1].Protocol(),
					nodes[k-1].overlay.String(): nodes[k-1].Protocol(),
				}),
			))
		}
	}
	return nodes
}
