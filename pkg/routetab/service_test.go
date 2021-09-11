package routetab_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/shed"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/discovery/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	p2pmock "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
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

type Network struct {
	ctx    context.Context
	server *Node
	client *Node
}

func newNetwork(t *testing.T, ctx context.Context) *Network {
	t.Helper()

	server := newTestNode(t)
	client := newTestNode(t)

	server.addOne(t, client.addr, true)
	client.addOne(t, server.addr, true)

	server.stream.SetProtocols(client.Protocol())
	client.stream.SetProtocols(server.Protocol())

	return &Network{ctx: ctx, server: server, client: client}
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
	//err = kad.Start(context.TODO())
	//if err != nil {
	//	t.Fatal(err)
	//}
	stream := streamtest.New(streamtest.WithBaseAddr(base.Overlay))
	service := routetab.New(base.Overlay, context.Background(), p2ps, kad, mockstate.NewStateStore(), noopLogger)
	service.SetConfig(routetab.Config{
		AddressBook: ab,
		NetworkID:   networkId,
		LightNodes:  lightnode.NewContainer(base.Overlay),
		Stream:      stream,
	})

	p2ps.SetPickyNotifier(kad)

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

func TestRouteTableWriteConflict(t *testing.T) {
	rt := routetab.NewRouteTable(mockstate.NewStateStore(), noopLogger, routetab.NewMetrics())

	target := test.RandomAddress()
	largePaths := make([]string, 1001)
	for i := 0; i < len(largePaths); i++ {
		largePaths[i] = test.RandomAddress().String()
	}

	rt.Set(target, []routetab.RouteItem{generateRoute([]string{largePaths[0]})})

	go func() {
		for i := 1; i < len(largePaths); i++ {
			rt.Set(target, []routetab.RouteItem{generateRoute([]string{largePaths[i]})})
		}
	}()

	cur := 0
	for i := 0; i < len(largePaths); i++ {
		route, err := rt.Get(target)
		if err != nil {
			t.Fatal(err)
		}
		if len(route) < cur {
			t.Fatalf("current route number expected to %d, got %d\n", cur, len(route))
		} else {
			cur = len(route)
		}
	}
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
	route := routetab.NewRouteTable(mockstate.NewStateStore(), noopLogger, routetab.NewMetrics())

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

func TestService_FindRoute(t *testing.T) {
	ctx := context.Background()
	ns := newNetwork(t, ctx)

	target, _ := randomAddress(t)

	p1 := test.RandomAddress()
	p2 := test.RandomAddress()
	item := generateRoute([]string{p1.String(), p2.String()})
	routes := []routetab.RouteItem{item}

	ns.server.addOne(t, target, false)

	err := ns.server.RouteTab().Set(target.Overlay, routes)
	if err != nil {
		t.Fatal(err)
	}

	dest, route, err := ns.client.FindRoute(ctx, target.Overlay)
	if err != nil {
		t.Fatalf("GetRoute err %s", err)
	}

	addr, _ := ns.server.book.Get(target.Overlay)
	if !dest.Equal(addr) {
		t.Fatalf("client receive aurora address not equal.")
	}
	receive := route[0]
	if receive.TTL != 3 || !receive.Neighbor.Equal(ns.server.overlay) {
		t.Fatalf("client receive route expired ttl=3,neighbor=%s, got ttl=%d,neighbor=%s", ns.server.overlay.String(), receive.TTL, receive.Neighbor.String())
	}
	if !receive.NextHop[0].Neighbor.Equal(p1) {
		t.Fatalf("client receive route expired nextHop neighbor=%s, got %s", p1.String(), receive.NextHop[0].Neighbor.String())
	}

	get, err := ns.client.RouteTab().Get(target.Overlay)
	if err != nil {
		t.Fatalf("client receive route err %s", err.Error())
	}
	if len(get) != 1 {
		t.Fatalf("client receive route expired count 1, got %d", len(get))
	}
	receive = get[0]
	if receive.TTL != 3 || !receive.Neighbor.Equal(ns.server.overlay) {
		t.Fatalf("client receive route expired ttl=3,neighbor=%s, got ttl=%d,neighbor=%s", ns.server.overlay, receive.TTL, receive.Neighbor.String())
	}
	if !receive.NextHop[0].Neighbor.Equal(p1) {
		t.Fatalf("client receive route expired nextHop neighbor=%s, got %s", p1.String(), receive.NextHop[0].Neighbor.String())
	}
}

// target in client neighbor
func TestService_FindRoute2(t *testing.T) {
	ctx := context.Background()
	ns := newNetwork(t, ctx)

	target, _ := randomAddress(t)
	ns.client.addOne(t, target, true)

	dest, route, err := ns.server.FindRoute(ctx, target.Overlay)
	if err != nil {
		t.Fatalf("client receive route err %s", err.Error())
	}
	addr, _ := ns.client.book.Get(target.Overlay)
	if !dest.Equal(addr) {
		t.Fatalf("client receive aurora address not equal.")
	}

	if len(route) != 1 {
		t.Fatalf("client receive route expired count 1, got %d", len(route))
	}
	receive := route[0]
	if receive.TTL != 1 || !receive.Neighbor.Equal(ns.client.overlay) {
		t.Fatalf("client receive route expired ttl=1,neighbor=%s, got ttl=%d,neighbor=%s", ns.client.overlay.String(), receive.TTL, receive.Neighbor.String())
	}
	if len(receive.NextHop) != 0 {
		t.Fatalf("client receive route expired len(nextHop)==0, got %d", len(receive.NextHop))
	}
}

// client have route
func TestService_FindRoute3(t *testing.T) {
	ctx := context.Background()
	ns := newNetwork(t, ctx)

	ns.server.addOne(t, ns.client.addr, true)
	target2, _ := randomAddress(t)
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()
	item := generateRoute([]string{p1.String(), p2.String()})
	routes := []routetab.RouteItem{item}
	ns.client.addOne(t, target2, false)
	err := ns.client.RouteTab().Set(target2.Overlay, routes)
	if err != nil {
		t.Fatalf("routetab set err %s", err.Error())
		return
	}

	dest, route, err := ns.server.FindRoute(ctx, target2.Overlay)
	if err != nil {
		t.Fatalf("client receive route err %s", err.Error())
	}
	addr, _ := ns.server.book.Get(target2.Overlay)
	if !dest.Equal(addr) {
		t.Fatalf("client receive aurora address not equal.")
	}

	receive := route[0]
	if receive.TTL != 3 || !receive.Neighbor.Equal(ns.client.overlay) {
		t.Fatalf("client receive route expired ttl=3,neighbor=%s, got ttl=%d,neighbor=%s", ns.client.overlay, receive.TTL, receive.Neighbor.String())
	}
	if !receive.NextHop[0].Neighbor.Equal(p1) {
		t.Fatalf("client receive route expired nextHop neighbor=%s, got %s", p1.String(), receive.NextHop[0].Neighbor.String())
	}
}

func TestHandleMaxTTLResponse(t *testing.T) {
	ctx := context.Background()
	ns := newNetwork(t, ctx)

	paths := make([]string, routetab.MaxTTL+1)
	for i := 0; i < len(paths); i++ {
		paths[i] = test.RandomAddress().String()
	}

	n1, _ := randomAddress(t)
	n2, _ := randomAddress(t)
	target, _ := randomAddress(t)
	serverSideRoute := generateRoute([]string{n1.Overlay.String(), n2.Overlay.String(), target.Overlay.String()})
	err := ns.server.RouteTab().Set(target.Overlay, []routetab.RouteItem{serverSideRoute})
	if err != nil {
		t.Fatal(err)
	}
	ns.server.addOne(t, target, false)
	request := pb.FindRouteReq{
		Dest: target.Overlay.Bytes(),
		Path: convPathByte(paths),
	}
	ch := make(chan struct{}, 1)
	ns.client.DoReq(ctx, ns.server.overlay, ns.server.peer, target.Overlay, &request, ch)

	time.Sleep(time.Millisecond * 10)

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
	iterate(serverRoute[0], len(paths)-1)

	_, err = ns.client.RouteTab().Get(target.Overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestHandleCirclePathResponse(t *testing.T) {
	ctx := context.Background()

	buf := bytes.NewBuffer([]byte{})
	createNS := func() *Network {
		oldLogger := noopLogger
		noopLogger = logging.New(buf, logrus.TraceLevel)
		defer func() {
			noopLogger = oldLogger
		}()

		return newNetwork(t, ctx)
	}
	ns := createNS()

	p1, _ := randomAddress(t)
	p2, _ := randomAddress(t)
	paths := []string{p1.Overlay.String(), ns.server.overlay.String(), p2.Overlay.String()}

	target, _ := randomAddress(t)
	request := pb.FindRouteReq{
		Dest: target.Overlay.Bytes(),
		Path: convPathByte(paths),
	}
	ns.client.DoReq(ctx, p2.Overlay, ns.server.peer, target.Overlay, &request, nil)
	time.Sleep(time.Millisecond * 50)
	_, _, err := ns.client.GetRoute(ctx, target.Overlay)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("exp target route notfound, got %v", err)
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
		//t.Log(string(line))
		exists := bytes.Contains(line, []byte(fmt.Sprintf("%s discard,", target.Overlay.String())))
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
	pend := routetab.NewPendCallResTab(addr, noopLogger, routetab.NewMetrics())

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

type noopService struct {
	err      error
	response pb.FindRouteResp
}

func (s *noopService) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    routetab.ProtocolName,
		Version: routetab.ProtocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name: routetab.StreamFindRouteReq,
				Handler: func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
					return nil
				},
			},
			{
				Name: routetab.StreamFindRouteResp,
				Handler: func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
					r := protobuf.NewReader(stream)
					defer func() {
						stream.FullClose()
					}()

					s.err = r.ReadMsgWithContext(ctx, &s.response)

					return nil
				},
			},
		},
	}
}

func TestBusyNetworkResponse(t *testing.T) {
	routetab.PendingTimeout = 1 * time.Minute
	defer func() {
		routetab.PendingTimeout = time.Second * 10
	}()

	ctx := context.Background()
	ns := newNetwork(t, ctx)

	target, _ := randomAddress(t)

	beforeClient := test.RandomAddress()
	beforeClientService := &noopService{}
	_ = streamtest.New(
		streamtest.WithProtocols(ns.client.Protocol()),
		streamtest.WithBaseAddr(beforeClient),
	)

	ns.client.stream.SetProtocols(beforeClientService.Protocol())

	paths := make([]string, 3)
	for i := 0; i < len(paths); i++ {
		paths[i] = test.RandomAddress().String()
	}
	paths = append(paths, beforeClient.String())
	request := pb.FindRouteReq{
		Dest: target.Overlay.Bytes(),
		Path: convPathByte(paths),
	}
	ns.client.DoReq(ctx, beforeClient, ns.server.peer, target.Overlay, &request, nil)

	time.Sleep(50 * time.Millisecond)

	beforeTarget := test.RandomAddress()
	paths = []string{beforeTarget.String(), target.Overlay.String()}
	route := generateRoute(paths)
	ns.server.addOne(t, target, true)
	aur, _ := ns.server.book.Get(target.Overlay)
	ns.server.DoResp(ctx, ns.client.peer, aur, []routetab.RouteItem{route})

	time.Sleep(50 * time.Millisecond)

	if beforeClientService.err != nil {
		t.Fatal(beforeClientService.err)
	}

	if !bytes.Equal(beforeClientService.response.Dest, target.Overlay.Bytes()) {
		t.Fatalf("expected to route destination %s, got. %s\n", target.Overlay, boson.NewAddress(beforeClientService.response.Dest))
	}

	var (
		iterate func(r *pb.RouteItem)
		index   int
	)
	expectedPaths := []string{
		ns.server.overlay.String(),
		beforeTarget.String(),
		target.Overlay.String(),
	}

	iterate = func(r *pb.RouteItem) {
		if hex.EncodeToString(r.Neighbor) != expectedPaths[index] {
			t.Fatalf("expected to route address %s, got %s\n", expectedPaths[index], hex.EncodeToString(r.Neighbor))
		}

		if len(r.NextHop) == 0 {
			return
		}

		index++
		iterate(r.NextHop[0])
	}

	iterate(beforeClientService.response.RouteItems[0])
}

func TestConnectFarNode(t *testing.T) {
	ctx := context.Background()

	ns := newNetwork(t, ctx)
	gap := newTestNode(t)

	ns.server.addOne(t, gap.addr, true)

	_ = ns.client.kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if address.Equal(gap.overlay) {
			t.Fatal("unexpected to find node gap")
		}
		return false, false, nil
	})
	_, err := ns.client.book.Get(gap.overlay)
	if !errors.Is(err, addressbook.ErrNotFound) {
		t.Fatal("node client should known gap")
	}

	err = ns.client.Connect(ctx, gap.overlay)
	if err != nil {
		t.Fatal(err)
	}

	auroraAddress, _, err := ns.client.GetRoute(ctx, gap.overlay)
	if errors.Is(err, routetab.ErrNotFound) {
		t.Fatal("expected to find to gap route, got notfound")
	}
	if !auroraAddress.Equal(gap.addr) {
		t.Fatal("expected to find aurora address eq gap")
	}
}

func TestGetClosestNeighbor(t *testing.T) {
	path := []string{"0301", "0304", "0503", "0604", "0905", "0406"}
	route := generateRoute(path)
	path1 := []string{"0302", "0304", "0503", "0704", "0905", "0506"}
	route1 := generateRoute(path1)
	path2 := []string{"0303", "0305", "0403", "0804", "0905", "0606"}
	route3 := generateRoute(path2)
	list := []routetab.RouteItem{route, route1, route3}
	got := routetab.GetClosestNeighbor(list)
	if len(got) != 3 {
		t.Fatalf("expected len(GetClosestNeighbor) 3 got %d", len(got))
	}
	if !got[0].Equal(boson.MustParseHexAddress("0406")) {
		t.Fatalf("expected address 0406  got %s", got[0].String())
	}
	if !got[1].Equal(boson.MustParseHexAddress("0506")) {
		t.Fatalf("expected address 0506  got %s", got[0].String())
	}
	if !got[2].Equal(boson.MustParseHexAddress("0606")) {
		t.Fatalf("expected address 0606  got %s", got[0].String())
	}
}
