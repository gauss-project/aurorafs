package hive2_test

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/crypto"
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
	"time"
)

const underlayBase = "/ip4/127.0.0.1/tcp/1634/dns/"

var (
	nonConnectableAddress, _        = ma.NewMultiaddr(underlayBase + "16Uiu2HAkx8ULY8cTXhdVAcMmLcH9AsTKz6uBQ7DPLKRjMLgBVYkA")
	noopLogger                      = logging.New(io.Discard, logrus.TraceLevel)
	networkId                uint64 = 0
)

type Node struct {
	overlay boson.Address
	addr    *aurora.Address
	peer    p2p.Peer
	signer  crypto.Signer
	book    addressbook.Interface
	p2ps    p2p.Service
	kad     *kademlia.Kad
	stream  *streamtest.Recorder
	*hive2.Service
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
		p2pmock.WithPeersFunc(func() (out []p2p.Peer) {
			_ = ab.IterateOverlays(func(address boson.Address) (bool, error) {
				out = append(out, p2p.Peer{
					Address: address,
					Mode:    aurora.NewModel().SetMode(aurora.FullNode),
				})
				return false, nil
			})
			return out
		}),
	)

	return p2ps
}

func newTestNode(t *testing.T, peer boson.Address, po int) *Node {
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

	base, signer := randomAddress(t, peer, po)
	ab := addressbook.New(mockstate.NewStateStore()) // address book
	p2ps := p2pMock(ab, base.Overlay, signer)

	stream := streamtest.New(streamtest.WithBaseAddr(base.Overlay))
	Hive2 := hive2.New(stream, ab, networkId, noopLogger)

	kad, err := kademlia.New(base.Overlay, ab, Hive2, p2ps, metricsDB, noopLogger, kademlia.Options{BinMaxPeers: 5, NodeMode: aurora.NewModel().SetMode(aurora.FullNode)}) // kademlia instance
	if err != nil {
		t.Fatal(err)
	}
	p2ps.SetPickyNotifier(kad)

	Hive2.SetAddPeersHandler(kad.AddPeers)
	Hive2.SetConfig(hive2.Config{
		Kad:  kad,
		Base: base.Overlay,
	})

	return &Node{
		overlay: base.Overlay,
		addr:    base,
		peer:    p2p.Peer{Address: base.Overlay},
		signer:  signer,
		book:    ab,
		p2ps:    p2ps,
		kad:     kad,
		stream:  stream,
		Service: Hive2,
	}
}

func randomAddress(t *testing.T, base boson.Address, po int) (addr *aurora.Address, signer crypto.Signer) {
	pk, _ := crypto.GenerateSecp256k1Key()
	signer = crypto.NewDefaultSigner(pk)

	p := test.RandomAddressAt(base, po)
	//base, _ := crypto.NewOverlayAddress(pk.PublicKey, networkId)
	mu, err := ma.NewMultiaddr(underlayBase + base.String())
	if err != nil {
		t.Fatal(err)
	}
	auroraAddr, err := aurora.NewAddress(signer, mu, p, networkId)
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

func (s *Node) connect(t *testing.T, peer boson.Address) {
	t.Helper()
	multiaddr, err := ma.NewMultiaddr(underlayBase + peer.String())
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := crypto.GenerateSecp256k1Key()
	signer := crypto.NewDefaultSigner(pk)

	auroraAddr, err := aurora.NewAddress(signer, multiaddr, peer, networkId)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.book.Put(peer, *auroraAddr); err != nil {
		t.Fatal(err)
	}
	err = s.kad.Connected(context.TODO(), p2p.Peer{Address: peer}, true)
	if err != nil {
		t.Fatal(err)
	}
}

func (s *Node) connectMore(t *testing.T, base boson.Address, po, count int) (list []boson.Address) {
	for i := 0; i < count; i++ {
		p3 := test.RandomAddressAt(base, po)
		s.connect(t, p3)
		list = append(list, p3)
		//t.Logf("po=%d %s", po, p3.String())
	}
	return
}

func TestLookupDistances(t *testing.T) {
	a := test.RandomAddress()

	t.Run("normal distances calc", func(t *testing.T) {
		b := test.RandomAddressAt(a, 1)
		res := hive2.LookupDistances(a, b)
		if len(res) != 3 {
			t.Fatalf("exp len(res)=3, got %d", len(res))
		}
		if res[0] != 1 {
			t.Fatalf("exp po=1, got %d", res[0])
		}
		if res[1] != 2 {
			t.Fatalf("exp po=2, got %d", res[1])
		}
		if res[2] != 0 {
			t.Fatalf("exp po=0, got %d", res[2])
		}
	})

	t.Run("most left distances calc", func(t *testing.T) {
		b := test.RandomAddressAt(a, 0)
		res := hive2.LookupDistances(a, b)
		if len(res) != 3 {
			t.Fatalf("exp len(res)=3, got %d", len(res))
		}
		if res[0] != 0 {
			t.Fatalf("exp po=0, got %d", res[0])
		}
		if res[1] != 1 {
			t.Fatalf("exp po=1, got %d", res[1])
		}
		if res[2] != 2 {
			t.Fatalf("exp po=2, got %d", res[2])
		}
	})

	t.Run("most right distances calc", func(t *testing.T) {
		b := test.RandomAddressAt(a, 31)
		res := hive2.LookupDistances(a, b)
		if len(res) != 3 {
			t.Fatalf("exp len(res)=3, got %d", len(res))
		}
		if res[0] != 31 {
			t.Fatalf("exp po=31, got %d", res[0])
		}
		if res[1] != 30 {
			t.Fatalf("exp po=30, got %d", res[1])
		}
		if res[2] != 29 {
			t.Fatalf("exp po=29, got %d", res[2])
		}
	})

}

func checkChan(t *testing.T, ch chan boson.Address, list []boson.Address) (total int) {
	t.Helper()
	skip := make([]boson.Address, 0)
	for {
		addr := <-ch
		if addr.IsZero() {
			return
		}
		if !addr.MemberOf(list) || addr.MemberOf(skip) {
			t.Fatalf("received expected find %s, got nil", addr.String())
		}
		skip = append(skip, addr)
		total++
	}
}

func TestService_DoFindNode(t *testing.T) {
	ctx := context.Background()

	a := newTestNode(t, test.RandomAddress(), -1)
	b := newTestNode(t, test.RandomAddress(), -1)

	a.addOne(t, b.addr, true)
	b.addOne(t, a.addr, true)

	a.stream.SetProtocols(b.Protocol())
	b.stream.SetProtocols(a.Protocol())

	err := a.kad.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pos := hive2.LookupDistances(a.overlay, b.overlay)

	t.Run("skip self", func(t *testing.T) {
		// skip self
		res, err := a.DoFindNode(ctx, a.overlay, b.overlay, pos, 2)
		if err != nil {
			t.Fatal(err)
		}
		total := checkChan(t, res, nil)
		if total != 0 {
			t.Fatalf("exp received 0 peer, got %d", total)
		}
	})

	// connected 2
	p2List := b.connectMore(t, a.overlay, int(pos[0]), 3)
	t.Run("connected 2", func(t *testing.T) {
		s0 := a.kad.Snapshot()
		if s0.Connected != 1 {
			t.Fatalf("connected expected 1 got %d", s0.Connected)
		}

		res, err := a.DoFindNode(ctx, a.overlay, b.overlay, pos, 2)
		if err != nil {
			t.Fatal(err)
		}
		total := checkChan(t, res, p2List)
		if total != 2 {
			t.Fatalf("exp received 2 peer, got %d", total)
		}
		time.Sleep(time.Millisecond * 100)
		s01 := a.kad.Snapshot()
		if s01.Connected != 3 {
			t.Fatalf("connected expected 3 got %d", s01.Connected)
		}
	})
}

func TestService_DoFindNodeMax(t *testing.T) {
	ctx := context.Background()

	a := newTestNode(t, test.RandomAddress(), -1)
	b := newTestNode(t, test.RandomAddress(), -1)

	a.addOne(t, b.addr, true)
	b.addOne(t, a.addr, true)

	a.stream.SetProtocols(b.Protocol())
	b.stream.SetProtocols(a.Protocol())

	err := a.kad.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pos := hive2.LookupDistances(a.overlay, b.overlay)

	// connected max
	p3List := b.connectMore(t, a.overlay, int(pos[1]), 20)
	t.Run("connected max", func(t *testing.T) {
		posReq := []int32{pos[1]}
		res1, err := a.DoFindNode(ctx, a.overlay, b.overlay, posReq, 16)
		if err != nil {
			t.Fatal(err)
		}
		total := checkChan(t, res1, p3List)
		if total != 16 {
			t.Fatalf("exp received 16 peer, got %d", total)
		}
		time.Sleep(time.Millisecond * 100)
		s03 := a.kad.Snapshot()
		if s03.Connected != 17 {
			t.Fatalf("connected expected 17 got %d", s03.Connected)
		}
	})
}

func TestService_Lookup(t *testing.T) {
	ctx := context.Background()

	a := newTestNode(t, test.RandomAddress(), -1)
	b := newTestNode(t, a.overlay, 5)
	c := newTestNode(t, a.overlay, 5)

	a.addOne(t, b.addr, true)
	//b.addOne(t, a.addr, true)
	b.addOne(t, c.addr, true)
	//c.addOne(t, b.addr, true)

	a.SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(a.overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			b.overlay.String(): b.Protocol(),
			c.overlay.String(): c.Protocol(),
		}),
	))

	b.SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(b.overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			a.overlay.String(): a.Protocol(),
			c.overlay.String(): c.Protocol(),
		}),
	))
	c.SetStreamer(streamtest.New(
		streamtest.WithBaseAddr(c.overlay),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{
			b.overlay.String(): b.Protocol(),
			a.overlay.String(): a.Protocol(),
		}),
	))

	err := a.kad.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = b.kad.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = c.kad.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	//t.Logf("a %s", a.overlay)
	//t.Logf("b %s", b.overlay)
	//t.Logf("c %s", c.overlay)
	//pos := hive2.LookupDistances(a.overlay, b.overlay)
	//b.connectMore(t, a.overlay, int(pos[0]), 2)

	posC := hive2.LookupDistances(a.overlay, c.overlay)
	c.connectMore(t, a.overlay, int(posC[1]), 3)

	hive2.NewLookup(a.overlay, a.Service).Run()

	// skip saturated po
	time.Sleep(time.Millisecond * 100)
	s03 := a.kad.Snapshot()
	if s03.Connected != 4 {
		t.Fatalf("connected expected 4 got %d", s03.Connected)
	}
}
