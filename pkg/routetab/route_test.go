package routetab_test

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	beeCrypto "github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/discovery/mock"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	p2pmock "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	ma "github.com/multiformats/go-multiaddr"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestPathToRouteItem(t *testing.T) {
	count := 10
	addresses := make([][]byte, count)
	for i := 0; i < len(addresses); i++ {
		addresses[i] = test.RandomAddress().Bytes()
	}

	route := routetab.PathToRouteItem(addresses)[0]
	cur := len(addresses) - 1
	for {
		if !route.Neighbor.Equal(boson.NewAddress(addresses[cur])) {
			t.Fatalf("current route neighbor expected to address %s, got %s\n", addresses[cur], route.Neighbor.String())
		}

		next := route.NextHop
		if next == nil {
			break
		}

		route = next[0]
		cur--
	}
}

func generatePath(path []string) routetab.RouteItem {
	maxTTL := len(path)
	route := routetab.RouteItem{
		CreateTime: time.Now().Unix(),
		TTL:        1,
		Neighbor:   boson.MustParseHexAddress(path[len(path)-1]),
	}
	for i := maxTTL - 2; i >= 0; i-- {
		itemNew := routetab.RouteItem{
			CreateTime: time.Now().Unix(),
			TTL:        uint8(maxTTL - i),
			Neighbor:   boson.MustParseHexAddress(path[i]),
			NextHop:    []routetab.RouteItem{route},
		}
		route = itemNew
	}
	return route
}

func Test_generatePath(t *testing.T) {
	path := []string{"0301", "0302", "0303", "0304", "0305", "0306"}
	maxTTL := uint8(len(path))
	route := generatePath(path)
	cur := uint8(0)
	for {
		if route.Neighbor.String() != path[cur] {
			t.Fatalf("current route neighbor expected to address %s, got %s\n", path[cur], route.Neighbor.String())
		}
		if route.TTL != maxTTL-cur {
			t.Fatalf("current route ttl expected to %d, got %d\n", maxTTL-cur, route.TTL)
		}
		next := route.NextHop
		if next == nil {
			break
		}
		route = next[0]
		cur++
	}
}

func TestUpdateRouteItem(t *testing.T) {
	path1 := []string{"0301", "0302", "0303"}
	item1 := generatePath(path1)
	path2 := []string{"0301", "0304", "0305", "0303"}
	item2 := generatePath(path2)

	routes := routetab.UpdateRouteItem(item1, item2)
	if len(routes.NextHop) != 2 {
		t.Fatalf("current route NextHop expected to count 2, got %d\n", len(routes.NextHop))
	}
	if routes.NextHop[0].Neighbor.String() != "0304" {
		t.Fatalf("current route NextHop expected to 0304, got %s\n", routes.NextHop[0].Neighbor.String())
	}
	if routes.NextHop[1].Neighbor.String() != "0302" {
		t.Fatalf("current route NextHop expected to 0302, got %s\n", routes.NextHop[1].Neighbor.String())
	}
}

func TestRouteTable_Gc(t *testing.T) {
	path1 := []string{"0301", "0302", "0303"}
	item1 := generatePath(path1)

	now, updated := routetab.CheckExpired([]routetab.RouteItem{item1}, time.Second)

	if updated {
		t.Fatalf("current route gc expected to false, got true")
	}
	if len(now) == 0 {
		t.Fatalf("current route gc expected now count > 0, got 0")
	}
	<-time.After(time.Second * 1)
	item1.CreateTime++
	//item1.NextHop[0].CreateTime++
	item1.NextHop[0].NextHop[0].CreateTime++
	now, updated = routetab.CheckExpired([]routetab.RouteItem{item1}, time.Second)
	if !updated {
		t.Fatalf("current route gc expected to true, got false")
	}

	if len(now) == 0 || len(now[0].NextHop) != 0 {
		t.Fatalf("current route gc expected now count 1 and nexHop count 0, got count %d , nextHop count %d ", len(now), len(now[0].NextHop))
	}
	route := routetab.NewRouteTable(mockstate.NewStateStore(), logging.New(os.Stdout, 0))

	item2 := generatePath(path1)
	target := test.RandomAddress()
	err := route.Set(target, []routetab.RouteItem{item2})
	if err != nil {
		t.Fatalf("routetab set err %s", err)
	}
	_, err = route.Get(target)
	if err != nil {
		t.Fatalf("routetab get err %s", err)
		return
	}
	route.Gc(time.Second)
	_, err = route.Get(target)
	if err != nil {
		t.Fatalf("routetab get err %s", err)
		return
	}
	<-time.After(time.Second * 1)
	route.Gc(time.Second)
	_, err = route.Get(target)
	if err != routetab.ErrNotFound {
		t.Fatalf("routetab get ,expected route: not found, got %s", err)
		return
	}
}

func TestService_Req(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	serverAddr := test.RandomAddress()
	clientAddr := test.RandomAddress()

	ctx := context.TODO()
	kad, signer, ab := newTestKademlia(serverAddr)
	addOne(t, signer, kad, ab, clientAddr)
	serverStream := streamtest.New()
	server := routetab.New(serverAddr, ctx, serverStream, kad, mockstate.NewStateStore(), logging.New(os.Stdout, 0))

	clientStream := streamtest.New(streamtest.WithProtocols(server.Protocol())) // connect to server
	kad2, signer1, ab1 := newTestKademlia(clientAddr)
	addOne(t, signer1, kad2, ab1, serverAddr)
	client := routetab.New(clientAddr, ctx, clientStream, kad2, mockstate.NewStateStore(), logging.New(os.Stdout, 0))

	serverStream.SetProtocols(client.Protocol()) // connect to client

	// origin path
	//path := []string{"0301", "0302"}

	target := test.RandomAddress()

	p1 := test.RandomAddress()
	p2 := test.RandomAddress()
	item := generatePath([]string{p1.String(), p2.String()})
	routes := []routetab.RouteItem{item}
	server.DoResp(ctx, p2p.Peer{Address: clientAddr}, target, routes)
	get, err := client.RouteTab().Get(target)
	if err != nil {
		t.Fatalf("client receive route err %s", err.Error())
	}
	if len(get) != 1 {
		t.Fatalf("client receive route expired count 1, got %d", len(get))
	}
	receive := get[0]
	if receive.TTL != 3 || !receive.Neighbor.Equal(serverAddr) {
		t.Fatalf("client receive route expired ttl=3,neighbor=%s, got ttl=%d,neighbor=%s", serverAddr.String(), receive.TTL, receive.Neighbor.String())
	}
}

func newTestKademlia(base boson.Address) (*kademlia.Kad, beeCrypto.Signer, addressbook.Interface) {

	var (
		connCounter, failedConnCounter *int32
		pk, _                          = crypto.GenerateSecp256k1Key()                                                     // random private key
		signer                         = beeCrypto.NewDefaultSigner(pk)                                                    // signer
		ab                             = addressbook.New(mockstate.NewStateStore())                                        // address book
		p2ps                           = p2pMock(ab, signer, connCounter, failedConnCounter)                               // p2p mock
		disc                           = mock.NewDiscovery()                                                               // mock discovery protocol
		kad                            = kademlia.New(base, ab, disc, p2ps, logging.New(os.Stdout, 0), kademlia.Options{}) // kademlia instance
	)

	return kad, signer, ab
}

const underlayBase = "/ip4/127.0.0.1/tcp/1634/dns/"

var (
	nonConnectableAddress, _ = ma.NewMultiaddr(underlayBase + "16Uiu2HAkx8ULY8cTXhdVAcMmLcH9AsTKz6uBQ7DPLKRjMLgBVYkA")
)

func p2pMock(ab addressbook.Interface, signer beeCrypto.Signer, counter, failedCounter *int32) p2p.Service {
	p2ps := p2pmock.New(p2pmock.WithConnectFunc(func(ctx context.Context, addr ma.Multiaddr) (*aurora.Address, error) {
		if addr.Equal(nonConnectableAddress) {
			_ = atomic.AddInt32(failedCounter, 1)
			return nil, errors.New("non reachable node")
		}
		if counter != nil {
			_ = atomic.AddInt32(counter, 1)
		}

		addresses, err := ab.Addresses()
		if err != nil {
			return nil, errors.New("could not fetch addresbook addresses")
		}

		for _, a := range addresses {
			if a.Underlay.Equal(addr) {
				return &a, nil
			}
		}

		address := test.RandomAddress()
		bzzAddr, err := aurora.NewAddress(signer, addr, address, 0)
		if err != nil {
			return nil, err
		}

		if err := ab.Put(address, *bzzAddr); err != nil {
			return nil, err
		}

		return bzzAddr, nil
	}))

	return p2ps
}

func addOne(t *testing.T, signer beeCrypto.Signer, k *kademlia.Kad, ab addressbook.Putter, peer boson.Address) {
	t.Helper()
	multiaddr, err := ma.NewMultiaddr(underlayBase + peer.String())
	if err != nil {
		t.Fatal(err)
	}
	bzzAddr, err := aurora.NewAddress(signer, multiaddr, peer, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = ab.Put(peer, *bzzAddr); err != nil {
		t.Fatal(err)
	}
	_ = k.AddPeers(context.Background(), peer)
}

func convPathByte(path []string) [][]byte {
	out := make([][]byte, len(path))
	for k, v := range path {
		out[k] = boson.MustParseHexAddress(v).Bytes()
	}
	return out
}
