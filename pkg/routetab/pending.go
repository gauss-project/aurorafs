package routetab

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	PendingTimeout  = time.Second * 5
	pendingInterval = time.Millisecond * 500
)

type PendCallResItem struct {
	Src        boson.Address
	CreateTime time.Time
	ResCh      chan struct{}
}

type PendingCallResArray []*PendCallResItem

type pendCallResTab struct {
	respList map[common.Hash]PendingCallResArray
	reqList  sync.Map // key common.Hash = dest+next
	mu       sync.RWMutex
}

func newPendCallResTab() *pendCallResTab {
	return &pendCallResTab{
		respList: make(map[common.Hash]PendingCallResArray),
	}
}

func (pend *pendCallResTab) Delete(target, next boson.Address) {
	key := getTargetKey(target)
	pend.mu.Lock()
	delete(pend.respList, key)
	pend.mu.Unlock()
	pend.reqList.Delete(getPendingReqKey(target, next))
}

func (pend *pendCallResTab) Add(target, src, next boson.Address, resCh chan struct{}) (has bool) {
	pending := &PendCallResItem{
		Src:        src,
		CreateTime: time.Now(),
		ResCh:      resCh,
	}

	key := getTargetKey(target)

	pend.mu.Lock()
	res, ok := pend.respList[key]
	if ok {
		pend.respList[key] = append(res, pending)
	} else {
		pend.respList[key] = PendingCallResArray{pending}
	}
	pend.mu.Unlock()
	// If a find route already exists, no forwarding is required
	reqKey := getPendingReqKey(target, next)
	_, has = pend.reqList.Load(reqKey)
	if !has {
		pend.reqList.Store(reqKey, time.Now())
	}
	return
}

func (pend *pendCallResTab) Get(target, next boson.Address) PendingCallResArray {
	key := getTargetKey(target)
	pend.mu.Lock()
	defer pend.mu.Unlock()
	res, ok := pend.respList[key]
	if ok {
		delete(pend.respList, key)
		pend.reqList.Delete(getPendingReqKey(target, next))
		return res
	}
	return nil
}

func (pend *pendCallResTab) GcReqLog(expire time.Duration) {
	pend.reqList.Range(func(key, value interface{}) bool {
		t := value.(time.Time)
		if time.Since(t).Milliseconds() >= expire.Milliseconds() {
			pend.reqList.Delete(key)
		}
		return true
	})
}

func (pend *pendCallResTab) GcResItems(expire time.Duration) {
	pend.mu.Lock()
	list := pend.respList
	pend.mu.Unlock()
	for key, item := range list {
		if time.Since(item[0].CreateTime).Milliseconds() < expire.Milliseconds() {
			// If the first one doesn't expire, then the next ones don't expire
			continue
		}
		expireK := 0
		for k, v := range item {
			if time.Since(v.CreateTime).Milliseconds() < expire.Milliseconds() {
				expireK = k
				break
			}
		}
		pend.mu.Lock()
		if expireK == 0 {
			delete(pend.respList, key)
		} else {
			pend.respList[key] = item[expireK:]
		}
		pend.mu.Unlock()
	}
}
