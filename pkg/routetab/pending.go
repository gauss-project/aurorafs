package routetab

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
)

var (
	PendingTimeout  = time.Second
	pendingInterval = time.Millisecond * 500
)

type pendCallResItem struct {
	src        boson.Address
	createTime time.Time
	resCh      chan struct{}
}

type pendingCallResArray []pendCallResItem
type pendCallResTab struct {
	items   map[common.Hash]pendingCallResArray
	mu      sync.Mutex
	logger  logging.Logger
	addr    boson.Address
	metrics metrics
}

func newPendCallResTab(addr boson.Address, logger logging.Logger, met metrics) *pendCallResTab {
	return &pendCallResTab{
		items:   make(map[common.Hash]pendingCallResArray),
		mu:      sync.Mutex{},
		logger:  logger,
		addr:    addr,
		metrics: met,
	}
}

func (pend *pendCallResTab) Delete(target boson.Address) {
	mKey := common.BytesToHash(target.Bytes())

	pend.mu.Lock()
	defer pend.mu.Unlock()

	delete(pend.items, mKey)
}

func (pend *pendCallResTab) Add(target, src boson.Address, resCh chan struct{}) (has bool) {
	mKey := common.BytesToHash(target.Bytes())

	pending := pendCallResItem{
		src:        src,
		createTime: time.Now(),
		resCh:      resCh,
	}

	pend.mu.Lock()
	defer pend.mu.Unlock()

	res, ok := pend.items[mKey]
	if ok {
		now := pendingCallResArray{}
		for _, v := range res {
			if v.src.Equal(src) {
				continue
			}
			now = append(now, v)
		}
		pend.items[mKey] = append(now, pending)
		has = true
	} else {
		pend.items[mKey] = pendingCallResArray{pending}
	}
	return
}

func (pend *pendCallResTab) Forward(ctx context.Context, s *Service, target *aurora.Address, routes []RouteItem) error {
	mKey := common.BytesToHash(target.Overlay.Bytes())

	pend.mu.Lock()
	defer pend.mu.Unlock()

	res := pend.items[mKey]
	for _, v := range res {
		if !v.src.Equal(pend.addr) {
			// forward
			s.doRouteResp(ctx, v.src, target, routes)
		} else if v.resCh != nil {
			// sync return
			v.resCh <- struct{}{}
		}
	}
	delete(pend.items, mKey)
	return nil
}

func (pend *pendCallResTab) Gc(expire time.Duration) {
	for mKey, item := range pend.items {
		pend.mu.Lock()
		var now pendingCallResArray
		for i := len(item) - 1; i >= 0; i-- {
			if time.Since(item[i].createTime).Milliseconds() > expire.Milliseconds() {
				if i == len(item)-1 {
					delete(pend.items, mKey)
				} else {
					now = item[i:]
				}
				break
			}
		}
		if len(now) > 0 {
			pend.items[mKey] = now
		}
		pend.mu.Unlock()
	}
}
