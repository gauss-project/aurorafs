package routetab

import (
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
)

func convRouteToPbRouteList(srcList []RouteItem) []*pb.RouteItem {
	out := make([]*pb.RouteItem, len(srcList))
	for k, src := range srcList {
		out[k] = convRouteItemToPbRoute(src)
	}
	return out
}

func convRouteItemToPbRoute(src RouteItem) *pb.RouteItem {
	out := &pb.RouteItem{
		CreateTime: src.CreateTime,
		TTL:        uint32(src.TTL),
		Neighbor:   src.Neighbor.Bytes(),
		NextHop:    make([]*pb.RouteItem, len(src.NextHop)),
	}
	for k, v := range src.NextHop {
		out.NextHop[k] = convRouteItemToPbRoute(v)
	}
	return out
}

func convPbToRouteList(srcList []*pb.RouteItem) []RouteItem {
	out := make([]RouteItem, len(srcList))
	for k, src := range srcList {
		out[k] = convPbToRouteItem(src)
	}
	return out
}

func convPbToRouteItem(src *pb.RouteItem) RouteItem {
	out := RouteItem{
		CreateTime: src.CreateTime,
		TTL:        uint8(src.TTL),
		Neighbor:   boson.NewAddress(src.Neighbor),
		NextHop:    make([]RouteItem, len(src.NextHop)),
	}
	for k, v := range src.NextHop {
		out.NextHop[k] = convPbToRouteItem(v)
	}
	return out
}

func inPath(b []byte, path [][]byte) bool {
	s := string(b)
	for _, v := range path {
		if string(v) == s {
			return true
		}
	}
	return false
}

// example path [a,b,c,d,e] the first is dest
// return e-d-c-b
func pathToRouteItem(path [][]byte) (routes []RouteItem) {
	ttl := len(path) - 1
	if ttl < 1 {
		return
	}
	route := RouteItem{
		CreateTime: time.Now().Unix(),
		TTL:        1,
		Neighbor:   boson.NewAddress(path[1]),
	}
	for i := 2; i <= ttl; i++ {
		itemNew := RouteItem{
			CreateTime: time.Now().Unix(),
			TTL:        uint8(i),
			Neighbor:   boson.NewAddress(path[i]),
			NextHop:    []RouteItem{route},
		}
		route = itemNew
	}
	return []RouteItem{route}
}

func inNeighbor(addr boson.Address, items []RouteItem) (now []RouteItem, index int, has bool) {
	now = make([]RouteItem, 0)
	for _, v := range items {
		if time.Now().Unix()-v.CreateTime < gcTime.Milliseconds()/1000 {
			now = append(now, v)
			if v.Neighbor.Equal(addr) {
				// only one match
				has = true
				index = len(now) - 1
			}
		}
	}
	return
}

func mergeRouteList(nowList, oldList []RouteItem) (routes []RouteItem) {
	routesNew := make([]RouteItem, 0)
	for _, now := range nowList {
		tmp := make([]RouteItem, 0)
		for _, old := range oldList {
			if now.Neighbor.Equal(old.Neighbor) {
				now = updateRouteItem(now, old)
			} else {
				tmp = append(tmp, old)
			}
		}
		routesNew = append(routesNew, now)
		oldList = tmp
	}
	if len(oldList) > 0 {
		routesNew = append(routesNew, oldList...)
	}
	return routesNew
}

func updateRouteItem(now, old RouteItem) (route RouteItem) {
	if now.Neighbor.Equal(old.Neighbor) {
		if old.TTL > now.TTL {
			old.TTL = now.TTL
		}
		old.CreateTime = time.Now().Unix()
		for _, x := range now.NextHop {
			nowNext, index, has := inNeighbor(x.Neighbor, old.NextHop)
			if has {
				nowNext[index] = updateRouteItem(x, nowNext[index])
				old.NextHop = nowNext
			} else {
				old.NextHop = append(old.NextHop, x)
			}
		}
		route = old
	}
	return
}

func checkExpired(old []RouteItem, expire time.Duration) (now []RouteItem, updated bool) {
	now = make([]RouteItem, 0)
	for _, v := range old {
		if time.Now().Unix()-v.CreateTime < expire.Milliseconds()/1000 {
			v.NextHop, updated = checkExpired(v.NextHop, expire)
			now = append(now, v)
		} else {
			updated = true
		}
	}
	return
}

func minTTL(items []RouteItem) int {
	ttl := MaxTTL
	if len(items) == 0 {
		ttl = 0
	}
	for _, v := range items {
		if ttl > v.TTL {
			ttl = v.TTL
		}
	}
	return int(ttl)
}

func minTTLPb(items []*pb.RouteItem) uint8 {
	ttl := MaxTTL
	if len(items) == 0 {
		ttl = 0
	}
	for _, v := range items {
		if ttl > uint8(v.TTL) {
			ttl = uint8(v.TTL)
		}
	}
	return ttl
}

func GetClosestNeighbor(routes []RouteItem) []boson.Address {
	addresses := make([]boson.Address, 0)
	for _, v := range routes {
		if len(v.NextHop) == 0 {
			addresses = append(addresses, v.Neighbor)
			continue
		}
		addresses = append(addresses, GetClosestNeighbor(v.NextHop)...)
	}
	return addresses
}

func GetClosestNeighborLimit(routes []RouteItem, limit int) (out []boson.Address) {
	list := GetClosestNeighbor(routes)
	for k, v := range list {
		out = append(out, v)
		if k+1 == limit {
			return
		}
	}
	return
}
