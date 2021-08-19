package routetab

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gogf/gf/os/gmlock"
	"time"
)

var (
	ErrNotFound         = errors.New("route: not found")
	NeighborAlpha       = 2
	MaxTTL        uint8 = 7
	GcTime              = time.Minute * 10
	GcInterval          = time.Minute
)

// RouteItem
///									                  |  -- (nextHop)
///									   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
///      neighbor <--|
///					 |				                  |  -- (nextHop)
///					 |				   |-- neighbor --|
///                  |---- (nextHop) --|              |  -- (nextHop)
///					 |                 |--neighbor ....
type RouteItem struct {
	CreateTime int64
	TTL        uint8
	Neighbor   boson.Address
	NextHop    []RouteItem
}

type routeTable struct {
	items       map[common.Hash][]RouteItem
	prefix      string
	mu          *gmlock.Locker
	store       storage.StateStorer
	logger      logging.Logger
	lockTimeout time.Duration
	metrics
}

func newRouteTable(store storage.StateStorer, logger logging.Logger, met metrics) *routeTable {
	return &routeTable{
		items:       map[common.Hash][]RouteItem{},
		prefix:      protocolName,
		mu:          gmlock.New(),
		store:       store,
		logger:      logger,
		lockTimeout: time.Second * 5,
		metrics:     met,
	}
}

func (rt *routeTable) tryLock(key string) error {
	now := time.Now()
	for !rt.mu.TryRLock(key) {
		time.After(time.Millisecond * 50)
		if time.Since(now).Seconds() > rt.lockTimeout.Seconds() {
			rt.metrics.TotalErrors.Inc()
			rt.logger.Errorf("routeTable: %s try lock timeout", key)
			err := fmt.Errorf("try lock timeout")
			return err
		}
	}
	return nil
}

func (rt *routeTable) Set(target boson.Address, routes []RouteItem) error {
	dest := target.String()
	key := rt.prefix + dest
	err := rt.tryLock(key)
	if err != nil {
		return err
	}
	defer rt.mu.RUnlock(key)

	// store get
	//old := make([]RouteItem, 0)
	//err = rt.store.Get(key, &old)
	//if err != nil && err != ErrNotFound {
	//	err = fmt.Errorf("routeTable: Set %s store get error: %s", dest, err.Error())
	//	rt.logger.Errorf(err.Error())
	//	return err
	//}

	mKey := common.BytesToHash(target.Bytes())
	old, _ := rt.items[mKey]

	if len(old) > 0 {
		routes = mergeRouteList(routes, old)
	}

	rt.items[mKey] = routes

	// store put
	//err = rt.store.Put(key, routes)
	//if err != nil {
	//	rt.logger.Errorf("routeTable: Set %s store put error: %s", dest, err.Error())
	//}

	return err
}

func (rt *routeTable) Get(target boson.Address) (routes []RouteItem, err error) {
	dest := target.String()
	key := rt.prefix + dest
	err = rt.tryLock(key)
	if err != nil {
		return
	}
	defer rt.mu.RUnlock(key)

	// store get
	//err = rt.store.Get(key, &routes)
	//if err != nil {
	//	if err == storage.ErrNotFound {
	//		err = ErrNotFound
	//		return
	//	}
	//	err = fmt.Errorf("routeTable: Get %s store get error: %s", dest, err.Error())
	//	rt.logger.Errorf(err.Error())
	//}

	mKey := common.BytesToHash(target.Bytes())
	routes, _ = rt.items[mKey]
	if len(routes) == 0 {
		err = ErrNotFound
	}

	return
}

func (rt *routeTable) Gc(expire time.Duration) {
	for mKey, routes := range rt.items {
		key := rt.prefix + mKey.String()
		err := rt.tryLock(key)
		if err != nil {
			continue
		}
		now, updated := checkExpired(routes, expire)
		if updated {
			if len(now) > 0 {
				rt.items[mKey] = now
			} else {
				delete(rt.items, mKey)
			}
		}
		rt.mu.RUnlock(key)
	}
}

func (rt *routeTable) GcStore(expire time.Duration) {
	err := rt.store.Iterate(rt.prefix, func(target, value []byte) (stop bool, err error) {
		key := string(target)
		err = rt.tryLock(key)
		if err != nil {
			return false, err
		}
		defer rt.mu.RUnlock(key)
		routes := make([]RouteItem, 0)
		err = json.Unmarshal(value, &routes)
		if err != nil {
			return false, err
		}
		now, updated := checkExpired(routes, expire)
		if updated {
			if len(now) > 0 {
				err = rt.store.Put(key, now)
				if err != nil {
					return false, err
				}
			} else {
				err = rt.store.Delete(key)
				if err != nil {
					return false, err
				}
			}

		}
		return false, nil
	})
	if err != nil {
		rt.metrics.TotalErrors.Inc()
		rt.logger.Errorf("routeTable: gc err %s", err)
	}
}
