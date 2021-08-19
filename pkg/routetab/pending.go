package routetab

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gogf/gf/os/gmlock"
	"time"
)

var (
	PendingTimeout  = time.Second * 10
	PendingInterval = time.Second
)

type pendCallResItem struct {
	src        boson.Address
	createTime time.Time
	resCh      chan struct{}
}

type pendingCallResArray []pendCallResItem
type pendCallResTab struct {
	items       map[common.Hash]pendingCallResArray
	mu          *gmlock.Locker
	lockTimeout time.Duration
	logger      logging.Logger
	addr        boson.Address
	metrics
}

func newPendCallResTab(addr boson.Address, logger logging.Logger, met metrics) *pendCallResTab {
	return &pendCallResTab{
		items:       make(map[common.Hash]pendingCallResArray),
		mu:          gmlock.New(),
		lockTimeout: time.Second,
		logger:      logger,
		addr:        addr,
		metrics:     met,
	}
}

func (pend *pendCallResTab) tryLock(key string) error {
	now := time.Now()
	for !pend.mu.TryRLock(key) {
		time.After(time.Millisecond * 10)
		if time.Since(now).Seconds() > pend.lockTimeout.Seconds() {
			pend.metrics.TotalErrors.Inc()
			pend.logger.Errorf("pendCallResTab: %s try lock timeout", key)
			err := fmt.Errorf("try lock timeout")
			return err
		}
	}
	return nil
}

func (pend *pendCallResTab) Add(target, src boson.Address, resCh chan struct{}) error {
	key := target.String()
	pending := pendCallResItem{
		src:        src,
		createTime: time.Now(),
		resCh:      resCh,
	}
	mKey := common.BytesToHash(target.Bytes())

	if err := pend.tryLock(key); err != nil {
		return err
	}
	defer pend.mu.RUnlock(key)

	res, ok := pend.items[mKey]
	if ok {
		pend.items[mKey] = append(res, pending)
	} else {
		pend.items[mKey] = pendingCallResArray{pending}
	}
	return nil
}

func (pend *pendCallResTab) Forward(ctx context.Context, s *Service, target boson.Address, routes []RouteItem) error {
	key := target.String()
	if err := pend.tryLock(key); err != nil {
		return err
	}
	defer pend.mu.RUnlock(key)

	mKey := common.BytesToHash(target.Bytes())
	res := pend.items[mKey]
	for _, v := range res {
		if !v.src.Equal(pend.addr) {
			// forward
			s.doResp(ctx, p2p.Peer{Address: v.src}, target, routes)
		} else if v.resCh != nil {
			// sync return
			v.resCh <- struct{}{}
		}
	}
	delete(pend.items, mKey)
	return nil
}

func (pend *pendCallResTab) Gc(expire time.Duration) {
	for destKey, item := range pend.items {
		pend.mu.TryRLockFunc(destKey.String(), func() {
			var now pendingCallResArray
			for i := len(item) - 1; i >= 0; i-- {
				if time.Since(item[i].createTime).Seconds() >= expire.Seconds() {
					if i == len(item)-1 {
						delete(pend.items, destKey)
					} else {
						now = item[i:]
					}
					break
				}
			}
			if len(now) > 0 {
				pend.items[destKey] = now
			}
		})
	}
}
