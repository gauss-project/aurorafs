package routetab_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	beeCrypto "github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/discovery/mock"
	"github.com/gauss-project/aurorafs/pkg/kademlia"
	"github.com/gauss-project/aurorafs/pkg/kademlia/pslice"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	p2pmock "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const underlayBase = "/ip4/127.0.0.1/tcp/1634/dns/"

var (
	nonConnectableAddress, _ = ma.NewMultiaddr(underlayBase + "16Uiu2HAkx8ULY8cTXhdVAcMmLcH9AsTKz6uBQ7DPLKRjMLgBVYkA")
	nopLogger = logging.New(os.NewFile(0, os.DevNull), logrus.ErrorLevel)
)

type Node struct {
	routetab.Service
	signer crypto.Signer
	book addressbook.Interface
}

type Network struct {
	ctx context.Context
	server Node
	client Node
}

func newNetwork(t *testing.T) *Network {
	t.Helper()

	serverAddr := test.RandomAddress()
	clientAddr := test.RandomAddress()

	ctx := context.Background()
	kad, signer, ab := newTestKademlia(serverAddr)
	addOne(t, signer, kad, ab, clientAddr)
	serverStream := streamtest.New(streamtest.WithBaseAddr(serverAddr))
	r1 := routetab.New(serverAddr, ctx, serverStream, kad, mockstate.NewStateStore(), nopLogger)

	clientStream := streamtest.New(
		streamtest.WithProtocols(r1.Protocol()),
		streamtest.WithBaseAddr(clientAddr),
	) // connect to server
	kad2, signer1, ab1 := newTestKademlia(clientAddr)
	addOne(t, signer1, kad2, ab1, serverAddr)
	r2 := routetab.New(clientAddr, ctx, clientStream, kad2, mockstate.NewStateStore(), nopLogger)

	serverStream.SetProtocols(r2.Protocol()) // connect to client
	ns := &Network{ctx: ctx, server: Node{r1, signer, ab}, client: Node{r2, signer1, ab1}}

	return ns
}

func (n *Network) ServerPeer() p2p.Peer {
	return p2p.Peer{Address: n.server.Address()}
}

func (n *Network) ClientPeer() p2p.Peer {
	return p2p.Peer{Address: n.client.Address()}
}

func (n *Network) Start() {
	n.server.Kad().Start(n.ctx)
	n.client.Kad().Start(n.ctx)
}

func (n *Network) Close() {
	n.server.Kad().Close()
	n.client.Kad().Close()
}

func p2pMock(ab addressbook.Interface, signer beeCrypto.Signer) p2p.Service {
	p2ps := p2pmock.New(p2pmock.WithConnectFunc(func(ctx context.Context, underlay ma.Multiaddr) (*aurora.Address, error) {
		if underlay.Equal(nonConnectableAddress) {
			return nil, errors.New("non reachable node")
		}
		addresses, err := ab.Addresses()
		if err != nil {
			return nil, errors.New("could not fetch addresbook addresses")
		}

		for _, a := range addresses {
			if a.Underlay.Equal(underlay) {
				return &a, nil
			}
		}

		overlay := test.RandomAddress()
		bzzAddr, err := aurora.NewAddress(signer, underlay, overlay, 0)
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

func newTestKademlia(base boson.Address) (*kademlia.Kad, beeCrypto.Signer, addressbook.Interface) {

	var (
		saturationFuncImpl *func(bin uint8, peers, connected *pslice.PSlice) (bool, bool)
		saturationFunc     = func(bin uint8, peers, connected *pslice.PSlice) (bool, bool) {
			f := *saturationFuncImpl
			return f(bin, peers, connected)
		}

		pk, _                          = crypto.GenerateSecp256k1Key()                                                     // random private key
		signer                         = beeCrypto.NewDefaultSigner(pk)                                                    // signer
		ab                             = addressbook.New(mockstate.NewStateStore())                                        // address book
		p2ps                           = p2pMock(ab, signer)                               					               // p2p mock
		disc                           = mock.NewDiscovery()                                                               // mock discovery protocol
		kad                            = kademlia.New(base, ab, disc, p2ps, nopLogger, kademlia.Options{SaturationFunc: saturationFunc, BitSuffixLength: 2}) // kademlia instance
	)

	sfImpl := func(bin uint8, peers, connected *pslice.PSlice) (bool, bool) {
		return kad.IsBalanced(bin), false
	}
	saturationFuncImpl = &sfImpl

	return kad, signer, ab
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

func generateRoute(path []string) routetab.RouteItem {
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

func Test_generateRoute(t *testing.T) {
	path := []string{"0301", "0302", "0303", "0304", "0305", "0306"}
	maxTTL := uint8(len(path))
	route := generateRoute(path)
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

func TestUpdateRouteItem(t *testing.T) {
	path1 := []string{"0301", "0302", "0303"}
	item1 := generateRoute(path1)
	path2 := []string{"0301", "0304", "0305", "0303"}
	item2 := generateRoute(path2)

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

func TestMergeRouteList(t *testing.T) {
	testCase := struct {
		oldRoutes         [][]string
		newRoutes         [][]string
		expectedNeighbors []string
		expectedPaths     [][]string
	}{
		//
		//  0001 → 0003 → 0008 → 0024
		//  ↓
		//  0006 → 0009 → 0025
		//  -------------
		// 0002 → 0011 → 0013 → 0014
		// ↓    ↘
		// 0018   0020
		// ↓       ↓
		// 0019 → 0021
		//  -------------
		// 0004 → 0026 → 0022 → 0021 → 0019
		//
		oldRoutes: [][]string{
			{"0001", "0003", "0008", "0024"},
			{"0001", "0006", "0009", "0025"},
			{"0002", "0011", "0013", "0014"},
			{"0002", "0018", "0019", "0021"},
			{"0002", "0020", "0021"},
			{"0004", "0026", "0022", "0021", "0019"},
		},
		// 0001 → 0003 → 0024
		// ↓
		// 0006 → 0007 → 0009 → 0025
		// -------------
		// 0002 → 0011
		//      ↘
		// 0018 ← 0020
		//      ↘
		//        0021
		// -------------
		// 0004 → 0026 → 0022 → 0021 → 0019
		// ↓
		// 0014 → 0013 → 0008
		newRoutes: [][]string{
			{"0001", "0003", "0024"},
			{"0001", "0006", "0007", "0009", "0025"},
			{"0002", "0011", "0020", "0018", "0021"},
			{"0004", "0026", "0022", "0021", "0019"},
			{"0004", "0014", "0013", "0008"},
		},
		expectedNeighbors: []string{"0001", "0002", "0004"},
	}

	generateRoutes := func(paths [][]string) []routetab.RouteItem {
		routeMap := make(map[string][][]string, 0)
		for _, path := range paths {
			_, exists := routeMap[path[0]]
			if !exists {
				routeMap[path[0]] = make([][]string, 0)
			}
			routeMap[path[0]] = append(routeMap[path[0]], path)
		}
		routeItems := make([]routetab.RouteItem, 0)
		for _, route := range routeMap {
			var prev routetab.RouteItem
			for i := 0; i < len(route); i++ {
				if i == 0 {
					prev = generateRoute(route[i])
				} else {
					prev = routetab.UpdateRouteItem(generateRoute(route[i]), prev)
				}
			}
			if len(route) > 0 {
				routeItems = append(routeItems, prev)
			}
		}
		return routeItems
	}

	oldRoutes := generateRoutes(testCase.oldRoutes)
	newRoutes := generateRoutes(testCase.newRoutes)
	routes := routetab.MergeRouteList(newRoutes, oldRoutes)

	neighbors := make(map[string]int)
	for i, route := range routes {
		neighbors[route.Neighbor.String()] = i
	}

	for _, expected := range testCase.expectedNeighbors {
		_, exists := neighbors[expected]
		if !exists {
			t.Fatalf("neighbor %s not found\n", expected)
		}
	}

	collectPaths := func(expected, routes []routetab.RouteItem) {
		var iterate func(w, r routetab.RouteItem)
		iterate = func(w, r routetab.RouteItem) {
			if !w.Neighbor.Equal(r.Neighbor) {
				t.Fatalf("expected to route neighbor %s, got %s\n", w.Neighbor, r.Neighbor)
			}

			nextHopMap := make(map[string]int)
			for i, next := range r.NextHop {
				nextHopMap[next.Neighbor.String()] = i
			}

			for _, expect := range w.NextHop {
				idx, exists := nextHopMap[expect.Neighbor.String()]
				if !exists {
					t.Fatalf("next route %s not found\n", expect.Neighbor)
				}
				iterate(expect, r.NextHop[idx])
			}
		}

		for _, route := range expected {
			iterate(route, routes[neighbors[route.Neighbor.String()]])
		}
	}

	collectPaths(oldRoutes, routes)
	collectPaths(newRoutes, routes)
}

func TestRouteTable_Gc(t *testing.T) {
	path1 := []string{"0301", "0302", "0303"}
	item1 := generateRoute(path1)

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
	route := routetab.NewRouteTable(mockstate.NewStateStore(), logging.New(os.Stdout, 0), routetab.NewMetrics())

	item2 := generateRoute(path1)
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
	ctx := context.Background()
	ns := newNetwork(t)

	t.Run("doResp && handlerResp can save route success", func(t *testing.T) {
		target := test.RandomAddress()

		p1 := test.RandomAddress()
		p2 := test.RandomAddress()
		item := generateRoute([]string{p1.String(), p2.String()})
		routes := []routetab.RouteItem{item}
		ns.server.DoResp(ctx, ns.ClientPeer(), target, routes)
		get, err := ns.client.RouteTab().Get(target)
		if err != nil {
			t.Fatalf("client receive route err %s", err.Error())
		}
		if len(get) != 1 {
			t.Fatalf("client receive route expired count 1, got %d", len(get))
		}
		receive := get[0]
		if receive.TTL != 3 || !receive.Neighbor.Equal(ns.server.Address()) {
			t.Fatalf("client receive route expired ttl=3,neighbor=%s, got ttl=%d,neighbor=%s", ns.server.Address().String(), receive.TTL, receive.Neighbor.String())
		}
		if !receive.NextHop[0].Neighbor.Equal(p1) {
			t.Fatalf("client receive route expired nextHop neighbor=%s, got %s", p1.String(), receive.NextHop[0].Neighbor.String())
		}
	})

}

func TestHandleMaxTTLResponse(t *testing.T) {
	ctx := context.Background()
	ns := newNetwork(t)

	paths := make([]string, routetab.MaxTTL + 1)
	for i := 0; i < len(paths); i++ {
		paths[i] = test.RandomAddress().String()
	}

	n1 := test.RandomAddress()
	n2 := test.RandomAddress()
	target := test.RandomAddress()
	serverSideRoute := generateRoute([]string{n1.String(), n2.String(), target.String()})
	ns.server.RouteTab().Set(target, []routetab.RouteItem{serverSideRoute})

	request := pb.FindRouteReq{
		Dest: target.Bytes(),
		Path: convPathByte(paths),
	}
	ch := make(chan struct{}, 0)
	ns.client.DoReq(ctx, ns.server.Address(), ns.ServerPeer(), target, &request, ch)

	time.Sleep(10 * time.Second)

	serverRoute, err := ns.server.RouteTab().Get(boson.MustParseHexAddress(paths[0]))
	if err != nil {
		t.Fatal(err)
	}

	var iterate func(r routetab.RouteItem, i int)
	iterate = func(r routetab.RouteItem, i int) {
		if r.Neighbor.String() != paths[i] {
			t.Fatalf("expected to route address %s, got %s\n", paths[i], r.Neighbor)
		}

		if len(r.NextHop) == 0 {
			return
		}

		i--
		iterate(r.NextHop[0], i)
	}
	iterate(serverRoute[0], len(paths) - 1)

	_, err = ns.client.RouteTab().Get(target)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestHandleCirclePathResponse(t *testing.T) {
	ctx := context.Background()

	buf := bytes.NewBuffer([]byte{})
	createNS := func() *Network {
		oldLogger := nopLogger
		nopLogger = logging.New(buf, logrus.TraceLevel)
		defer func() {
			nopLogger = oldLogger
		}()

		return newNetwork(t)
	}
	ns := createNS()

	ns.Start()
	defer ns.Close()

	paths := make([]string, 5)
	for i := 0; i < len(paths); i++ {
		paths[i] = test.RandomAddress().String()
	}

	circleHead := paths[rand.Intn(len(paths) - 1)]
	neighbor := boson.MustParseHexAddress(circleHead)
	addOne(t, ns.server.signer, ns.server.Kad(), ns.server.book, neighbor)

	target := test.RandomAddress()
	request := pb.FindRouteReq{
		Dest: target.Bytes(),
		Path: convPathByte(paths),
	}
	ns.client.DoReq(ctx, ns.server.Address(), ns.ServerPeer(), target, &request, nil)

	time.Sleep(5 * time.Second)

	_, err := ns.client.RouteTab().Get(target)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatal(err)
	}

	detect := true
	scanner := bufio.NewReader(buf)
	for {
		line, _, err := scanner.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		exists := bytes.Contains(line, []byte(fmt.Sprintf("handlerFindRouteReq dest= %s discard", target)))
		if exists {
			detect = false
		}
	}

	if detect {
		t.Fatalf("detect a circle in path\n")
	}
}

func TestPendCallResTab_Gc(t *testing.T) {
	addr := test.RandomAddress()
	pend := routetab.NewPendCallResTab(addr, logging.New(os.Stdout, 0), routetab.NewMetrics())

	target1 := test.RandomAddress()
	target2 := test.RandomAddress()
	src1 := test.RandomAddress()
	src2 := test.RandomAddress()
	err := pend.Add(target1, src1, nil)
	if err != nil {
		t.Fatalf("route: pending add %s", err)
	}
	err = pend.Add(target1, src2, nil)
	if err != nil {
		t.Fatalf("route: pending add %s", err)
	}
	err = pend.Add(target2, src2, nil)
	if err != nil {
		t.Fatalf("route: pending add %s", err)
	}
	pend.Gc(time.Second)

	items := pend.GetItems()
	tmp1, has := items[common.BytesToHash(target1.Bytes())]
	if !has {
		t.Fatalf("route peing expired have %s, got nil", target1.String())
	}
	if len(tmp1) != 2 {
		t.Fatalf("route peing expired have src count 2, got %d", len(tmp1))
	}
	if _, ok := items[common.BytesToHash(target2.Bytes())]; !ok {
		t.Fatalf("route peing expired have %s, got nil", target1.String())
	}
	<-time.After(time.Second)
	pend.Gc(time.Second)
	items2 := pend.GetItems()
	if len(items2) > 0 {
		t.Fatalf("route peing expired count 0, got %d", len(items2))
	}
}
