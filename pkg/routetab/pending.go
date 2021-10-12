package routetab

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	PendingTimeout  = time.Second
	pendingInterval = time.Millisecond * 500
)

type PendCallResItem struct {
	Src        boson.Address
	CreateTime time.Time
	ResCh      chan struct{}
}

type PendingCallResArray []*PendCallResItem
type pendCallResTab struct {
	items map[common.Hash]PendingCallResArray
	mu    sync.Mutex
}

func newPendCallResTab() *pendCallResTab {
	return &pendCallResTab{
		items: make(map[common.Hash]PendingCallResArray),
		mu:    sync.Mutex{},
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

	pending := &PendCallResItem{
		Src:        src,
		CreateTime: time.Now(),
		ResCh:      resCh,
	}

	pend.mu.Lock()
	defer pend.mu.Unlock()

	res, ok := pend.items[mKey]
	if ok {
		has = true
		for _, v := range res {
			if v.Src.Equal(src) {
				v.CreateTime = time.Now()
				return
			}
		}
		res = append(res, pending)
		pend.items[mKey] = res
	} else {
		pend.items[mKey] = PendingCallResArray{pending}
	}
	return
}

func (pend *pendCallResTab) Get(target boson.Address) PendingCallResArray {
	mKey := common.BytesToHash(target.Bytes())

	pend.mu.Lock()
	defer pend.mu.Unlock()

	res, ok := pend.items[mKey]
	if ok {
		delete(pend.items, mKey)
		return res
	}
	return nil
}

func (pend *pendCallResTab) Gc(expire time.Duration) {
	for mKey, item := range pend.items {
		pend.mu.Lock()
		var now PendingCallResArray
		var update bool
		for k, v := range item {
			if time.Since(v.CreateTime).Milliseconds() < expire.Milliseconds() {
				now = item[k:]
				if len(now) > 0 {
					pend.items[mKey] = now
					update = true
				}
				break
			}
		}
		if !update {
			delete(pend.items, mKey)
		}
		pend.mu.Unlock()
	}
}
