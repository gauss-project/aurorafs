package routetab

import (
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto/bls"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gogf/gf/util/gconv"
	"strings"
	"sync"
	"time"
)

const (
	routePrefix = "route_index_"
	pathPrefix  = "route_pathKey_"
)

var (
	ErrNotFound = errors.New("route: not found")
)

type TargetRoute struct {
	Neighbor boson.Address // nextHop
	PathKey  common.Hash
}

type Path struct {
	Sign       []byte          `json:"sign"`
	Bodys      [][]byte        `json:"bodys"`
	Items      []boson.Address `json:"items"`
	CreateTime time.Time       `json:"createTime"`
	UsedTime   time.Time       `json:"usedTime"`
}

type Table struct {
	self   boson.Address
	paths  sync.Map                      // key=sha256sum(path.items), value=*Path
	routes map[common.Hash][]TargetRoute // key=target
	mu     sync.RWMutex
	store  storage.StateStorer
}

func newRouteTable(self boson.Address, store storage.StateStorer) *Table {
	return &Table{
		self:   self,
		routes: make(map[common.Hash][]TargetRoute),
		store:  store,
	}
}

func (t *Table) convertPathsToPbPaths(path []*Path) []*pb.Path {
	body := gconv.Bytes(time.Now().Unix())
	out := make([]*pb.Path, 0)
	for _, v := range path {
		out = append(out, &pb.Path{
			Sign:  bls.Sign(body, v.Sign),
			Bodys: append(v.Bodys, body),
			Items: append(convItemsToBytes(v.Items), t.self.Bytes()),
		})
	}
	return out
}

func (t *Table) generatePaths(paths []*pb.Path) (out []*pb.Path) {
	body := gconv.Bytes(time.Now().Unix())
	if len(paths) == 0 {
		out = append(out, &pb.Path{
			Sign:  bls.Sign(body),
			Bodys: [][]byte{body},
			Items: [][]byte{t.self.Bytes()},
		})
	} else {
		for _, v := range paths {
			out = append(out, &pb.Path{
				Sign:  bls.Sign(body, v.Sign),
				Bodys: append(v.Bodys, body),
				Items: append(v.Items, t.self.Bytes()),
			})
		}
	}
	return out
}

func (t *Table) SavePaths(paths []*pb.Path) {
	for _, path := range paths {
		t.SavePath(path)
	}
}

func (t *Table) SavePath(p *pb.Path) {
	if !verifyPath(p) {
		return
	}
	if len(p.Items) < 2 {
		return
	}
	// save path
	pathKey, items := generatePathItems(p.Items)
	path := Path{
		Sign:       p.Sign,
		Bodys:      p.Bodys,
		Items:      items,
		CreateTime: time.Now(),
		UsedTime:   time.Now(),
	}
	t.paths.Store(pathKey, &path)
	_ = t.store.Put(pathPrefix+pathKey.String(), path)

	// parse route from path
	route := TargetRoute{Neighbor: items[len(items)-1], PathKey: pathKey}
	t.IterateTarget(items, func(target boson.Address) {
		targetKey := getTargetKey(target)
		t.mu.Lock()
		defer t.mu.Unlock()
		routes, ok := t.routes[targetKey]
		if ok {
			old := routes
			if existRoute(route, old) {
				return
			}
			routes = append(old, route)

		} else {
			routes = []TargetRoute{route}
		}
		t.routes[targetKey] = routes
		_ = t.store.Put(routePrefix+target.String(), routes)
	})
}

func (t *Table) IterateTarget(items []boson.Address, fn func(target boson.Address)) {
	length := len(items)
	for k, target := range items {
		if k <= length-2 {
			fn(target)
		}
	}
}

func (t *Table) Get(target boson.Address) ([]*Path, error) {
	targetKey := getTargetKey(target)
	t.mu.RLock()
	routes, ok := t.routes[targetKey]
	t.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	paths := make([]*Path, 0)
	for _, v := range routes {
		path, has := t.paths.Load(v.PathKey)
		if has {
			paths = append(paths, path.(*Path))
		}
	}
	if len(paths) == 0 {
		return nil, ErrNotFound
	}
	return paths, nil
}

func (t *Table) updateUsedTime(target, neighbor boson.Address) {
	targetKey := getTargetKey(target)
	t.mu.RLock()
	routes, ok := t.routes[targetKey]
	t.mu.RUnlock()
	if ok {
		for _, v := range routes {
			if !v.Neighbor.Equal(neighbor) {
				continue
			}
			path, has := t.paths.Load(v.PathKey)
			if has {
				p := path.(*Path)
				if time.Since(p.UsedTime) > 0 {
					p.UsedTime = time.Now()
				}
			}
		}
	}
}

func (t *Table) GetNextHop(target boson.Address) (next []boson.Address) {
	targetKey := getTargetKey(target)
	t.mu.RLock()
	routes, ok := t.routes[targetKey]
	t.mu.RUnlock()
	if ok {
		for _, v := range routes {
			next = append(next, v.Neighbor)
		}
	}
	return
}

func (t *Table) Gc(expire time.Duration) {
	t.paths.Range(func(key, value interface{}) bool {
		path := value.(*Path)
		if time.Since(path.UsedTime).Milliseconds() > expire.Milliseconds() {
			t.Delete(path)
		}
		return true
	})
}

func (t *Table) Delete(path *Path) {
	pathKey, _ := generatePathItems(convItemsToBytes(path.Items))
	// delete path
	t.paths.Delete(pathKey)
	_ = t.store.Delete(pathPrefix + pathKey.String())
	// delete routes
	t.IterateTarget(path.Items, func(target boson.Address) {
		targetKey := getTargetKey(target)
		t.mu.Lock()
		defer t.mu.Unlock()
		routes, ok := t.routes[targetKey]
		if ok {
			routesNow := make([]TargetRoute, 0)
			for _, v := range routes {
				if v.PathKey != pathKey {
					routesNow = append(routesNow, v)
				}
			}
			if len(routesNow) < len(routes) {
				t.routes[targetKey] = routesNow
			}
		}
	})
}

func (t *Table) ResumePaths() {
	_ = t.store.Iterate(pathPrefix, func(key, value []byte) (stop bool, err error) {
		hex := strings.TrimPrefix(string(key), pathPrefix)
		pathKey := common.HexToHash(hex)
		path := &Path{}
		err = json.Unmarshal(value, path)
		if err != nil {
			_ = t.store.Delete(string(key))
		} else {
			t.paths.Store(pathKey, path)
		}
		return false, nil
	})
}

func (t *Table) ResumeRoutes() {
	_ = t.store.Iterate(routePrefix, func(key, value []byte) (stop bool, err error) {
		hex := strings.TrimPrefix(string(key), routePrefix)
		targetKey := common.HexToHash(hex)
		var route []TargetRoute
		err = json.Unmarshal(value, &route)
		if err != nil {
			_ = t.store.Delete(routePrefix + hex)
		} else {
			t.mu.Lock()
			t.routes[targetKey] = route
			t.mu.Unlock()
		}
		return false, nil
	})
}


