package routetab_test

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"math"
	"math/rand"
	"testing"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().Unix()))

func generateAddress() boson.Address {
	b := make([]byte, boson.HashSize)
	for i := range b {
		b[i] = byte(r.Intn(math.MaxUint8+1))
	}
	return boson.NewAddress(b)
}

func TestPathToRouteItem(t *testing.T) {
	count := 10
	addresses := make([]boson.Address, count)
	for i := 0; i < len(addresses); i++ {
		addresses[i] = generateAddress()
	}

	route := routetab.PathToRouteItem(addresses)[0]
	cur := len(addresses) - 1
	for {
		if !route.GetNeighbor().Equal(addresses[cur]) {
			t.Fatalf("current route neighbor expected to address %s, got %s\n", addresses[cur], route.GetNeighbor())
		}

		next := route.GetNextHop()
		if next == nil {
			break
		}

		route = next[0]
		cur--
	}
}