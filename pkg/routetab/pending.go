package routetab

import (
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
	items  sync.Map
	reqLog sync.Map
}

func newPendCallResTab() *pendCallResTab {
	return &pendCallResTab{
		items:  sync.Map{},
		reqLog: sync.Map{},
	}
}

func (pend *pendCallResTab) Delete(target, next boson.Address) {
	key := target.ByteString()
	pend.items.Delete(key)
	pend.reqLog.Delete(key + next.ByteString())
}

func (pend *pendCallResTab) Add(target, src, next boson.Address, resCh chan struct{}) (has bool) {
	pending := &PendCallResItem{
		Src:        src,
		CreateTime: time.Now(),
		ResCh:      resCh,
	}

	key := target.ByteString()

	res, ok := pend.items.Load(key)
	if ok {
		list := res.(PendingCallResArray)
		list = append(list, pending)
		pend.items.Store(key, list)
	} else {
		pend.items.Store(key, PendingCallResArray{pending})
	}
	// If a find route already exists, no forwarding is required
	reqKey := key + next.ByteString()
	_, has = pend.reqLog.Load(reqKey)
	if !has {
		pend.reqLog.Store(reqKey, time.Now())
	}
	return
}

func (pend *pendCallResTab) Get(target, next boson.Address) PendingCallResArray {
	key := target.ByteString()
	res, ok := pend.items.Load(key)
	if ok {
		pend.items.Delete(key)
		pend.reqLog.Delete(key + next.ByteString())
		return res.(PendingCallResArray)
	}
	return nil
}

func (pend *pendCallResTab) GcReqLog(expire time.Duration) {
	pend.reqLog.Range(func(key, value interface{}) bool {
		t := value.(time.Time)
		if time.Since(t).Milliseconds() >= expire.Milliseconds() {
			pend.reqLog.Delete(key)
		}
		return true
	})
}

func (pend *pendCallResTab) GcResItems(expire time.Duration) {
	pend.items.Range(func(key, value interface{}) bool {
		item := value.(PendingCallResArray)
		if time.Since(item[0].CreateTime).Milliseconds() < expire.Milliseconds() {
			// If the first one doesn't expire, then the next ones don't expire
			return true
		}
		expireK := 0
		for k, v := range item {
			if time.Since(v.CreateTime).Milliseconds() < expire.Milliseconds() {
				expireK = k
				break
			}
		}
		if expireK == 0 {
			pend.items.Delete(key)
		} else {
			pend.items.Store(key, item[expireK:])
		}
		return true
	})
}
