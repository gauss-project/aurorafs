package routetab

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto/bls"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"strings"
	"sync"
	"time"
)

const (
	routePrefix  = "route_index_"
	signedPrefix = "route_signed_"
)

var (
	ErrNotFound = errors.New("route: not found")
)

type Route struct {
	Index []Index
}

type Index struct {
	NextHop boson.Address
	Sign    []byte
}

type Path struct {
	CreateTime int64           `json:"createTime"`
	Sign       []byte          `json:"sign"`
	Item       []boson.Address `json:"item"`
	UsedTime   time.Time       `json:"usedTime"`
}

type Table struct {
	self   boson.Address
	items  map[common.Hash]Route
	signed sync.Map

	mu    sync.RWMutex
	store storage.StateStorer
}

func newRouteTable(self boson.Address, store storage.StateStorer) *Table {
	return &Table{
		self:   self,
		items:  make(map[common.Hash]Route),
		mu:     sync.RWMutex{},
		store:  store,
	}
}

func (t *Table) ReqToResp(req *pb.RouteReq, paths []*Path) *pb.RouteResp {
	np := make([]*pb.Path, 0)
	if paths != nil {
		for _, path := range paths {
			item := make([][]byte, len(path.Item))
			for k, v := range path.Item {
				item[k] = v.Bytes()
			}
			newPath := make([][]byte, len(req.Path.Item)+len(path.Item))
			copy(newPath, req.Path.Item)
			copy(newPath[len(req.Path.Item):], item)

			np = append(np, &pb.Path{
				Sign: bls.Sign(path.Sign), // todo
				Item: newPath,
			})
		}
	} else {
		np = append(np, &pb.Path{
			Sign: bls.Sign(req.Path.Sign), // todo
			Item: append(req.Path.Item, t.self.Bytes()),
		})
	}

	resp := &pb.RouteResp{
		Dest:  req.Dest,
		Paths: np,
	}

	return resp
}

func (t *Table) ReqToSave(req *pb.RouteReq) error {
	items := make([][]byte, 0)
	items = append(items, t.self.Bytes())
	for i := len(req.Path.Item) - 1; i >= 0; i-- {
		items = append(items, req.Path.Item[i])
	}
	path := &pb.Path{
		Sign: bls.Sign(req.Path.Sign), // todo
		Item: items,
	}
	return t.RespToSaveOne(path)
}

func (t *Table) RespToSave(resp *pb.RouteResp) error {
	for _, v := range resp.Paths {
		err := t.RespToSaveOne(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) RespToSaveOne(p *pb.Path) error {
	items := make([]boson.Address, 0)
	start := false
	for _, v := range p.Item {
		if bytes.Equal(v, t.self.Bytes()) {
			start = true
		}
		if start {
			items = append(items, boson.NewAddress(v))
			if !bls.Verify(p.Sign) {
				return errors.New("sign verify failed")
			}
		}
	}
	signKey := common.BytesToHash(p.Sign)

	t.mu.Lock()
	defer t.mu.Unlock()

	path := Path{
		CreateTime: time.Now().Unix(),
		Sign:       p.Sign,
		Item:       items,
		UsedTime:   time.Now(),
	}
	t.signed.Store(signKey, &path)
	_ = t.store.Put(signedPrefix+signKey.String(), path)

	for i := 2; i < len(path.Item); i++ {
		destKey := common.BytesToHash(path.Item[i].Bytes())
		idx := Index{
			NextHop: path.Item[1],
			Sign:    path.Sign,
		}
		route, ok := t.items[destKey]
		if ok {
			has := false
			for _, v := range route.Index {
				if v.NextHop.Equal(idx.NextHop) && string(v.Sign) == string(idx.Sign) {
					has = true
					break
				}
			}
			if !has {
				route.Index = append(route.Index, idx)
			}
		} else {
			route = Route{Index: []Index{idx}}
		}
		t.items[destKey] = route
		_ = t.store.Put(routePrefix+destKey.String(), route)
	}
	return nil
}

func (t *Table) Get(dest boson.Address) ([]*Path, error) {
	mKey := common.BytesToHash(dest.Bytes())

	t.mu.RLock()
	route, ok := t.items[mKey]
	if !ok {
		t.mu.RUnlock()
		return nil, ErrNotFound
	}
	needUpdate := false
	newIndex := make([]Index, 0)
	paths := make([]*Path, 0)
	for _, v := range route.Index {
		signKey := common.BytesToHash(v.Sign)
		path, has := t.signed.Load(signKey)
		if has {
			paths = append(paths, path.(*Path))
			newIndex = append(newIndex, v)
		} else {
			needUpdate = true
		}
	}
	t.mu.RUnlock()

	if needUpdate {
		t.mu.Lock()
		route.Index = newIndex
		t.items[mKey] = route
		_ = t.store.Put(routePrefix+mKey.String(), route)
		t.mu.Unlock()
	}
	if len(paths) == 0 {
		return nil, ErrNotFound
	}
	return paths, nil
}

func (t *Table) GetNextHop(dest boson.Address, sign []byte) (next boson.Address) {
	mKey := common.BytesToHash(dest.Bytes())
	t.mu.RLock()
	route := t.items[mKey]
	delKey := -1
	for k, v := range route.Index {
		if bytes.Equal(v.Sign, sign) {
			signKey := common.BytesToHash(v.Sign)
			_, ok := t.signed.Load(signKey)
			if ok {
				next = v.NextHop
			} else {
				// update route index
				delKey = k
			}
			break
		}
	}
	t.mu.RUnlock()

	if delKey >= 0 {
		newIndex := make([]Index, len(route.Index)-1)
		copy(newIndex, route.Index[:delKey])
		copy(newIndex[delKey:], route.Index[delKey:])
		route.Index = newIndex
		t.mu.Lock()
		t.items[mKey] = route
		t.mu.Unlock()
	}
	return
}

func (t *Table) Gc(expire time.Duration) {
	t.signed.Range(func(key, value interface{}) bool {
		path := value.(*Path)
		if time.Since(path.UsedTime).Milliseconds() > expire.Milliseconds() {
			t.Delete(path)
		}
		return true
	})
}

func (t *Table) Delete(path *Path) {
	signKey := common.BytesToHash(path.Sign)
	t.signed.Delete(signKey)
	_ = t.store.Delete(signedPrefix + signKey.String())
}

func (t *Table) ResumeSigned() {
	_ = t.store.Iterate(signedPrefix, func(key, value []byte) (stop bool, err error) {
		hex := strings.TrimPrefix(string(key), signedPrefix)
		signKey := common.HexToHash(hex)
		path := &Path{}
		err = json.Unmarshal(value, path)
		if err != nil {
			_ = t.store.Delete(signedPrefix + hex)
		} else {
			t.signed.Store(signKey, path)
		}
		return false, nil
	})
}

func (t *Table) ResumeRoute() {
	_ = t.store.Iterate(routePrefix, func(key, value []byte) (stop bool, err error) {
		hex := strings.TrimPrefix(string(key), routePrefix)
		mKey := common.HexToHash(hex)
		route := Route{}
		err = json.Unmarshal(value, &route)
		if err != nil {
			_ = t.store.Delete(routePrefix + hex)
		} else {
			t.mu.Lock()
			t.items[mKey] = route
			t.mu.Unlock()
		}
		return false, nil
	})
}
