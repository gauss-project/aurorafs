package routetab_test

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/routetab"
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
	route := routetab.RouteItem{
		CreateTime: time.Now().Unix(),
		TTL:        uint8(len(path)),
		Neighbor:   boson.MustParseHexAddress(path[len(path)-1]),
	}
	for i := len(path) - 2; i >= 0; i-- {
		itemNew := routetab.RouteItem{
			CreateTime: time.Now().Unix(),
			TTL:        uint8(i) + 1,
			Neighbor:   boson.MustParseHexAddress(path[i]),
			NextHop:    []routetab.RouteItem{route},
		}
		route = itemNew
	}
	return route
}

func Test_generatePath(t *testing.T) {
	path := []string{"0301", "0302", "0303", "0304", "0305", "0306"}
	route := generatePath(path)
	cur := 0
	for {
		if route.Neighbor.String() != path[cur] {
			t.Fatalf("current route neighbor expected to address %s, got %s\n", path[cur], route.Neighbor.String())
		}
		if route.TTL != uint8(cur)+1 {
			t.Fatalf("current route ttl expected to %d, got %d\n", cur+1, route.TTL)
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

	routes := routetab.UpdateRouteItem(item1,item2)
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
