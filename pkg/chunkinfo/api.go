package chunkinfo

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"sync"
	"time"
)

func (ci *ChunkInfo) API() rpc.API {
	return rpc.API{
		Namespace: "chunkInfo",
		Version:   "1.0",
		Service:   &apiService{ci: ci},
		Public:    true,
	}
}

type apiService struct {
	ci *ChunkInfo
}

func (a *apiService) DownloadProgress(ctx context.Context, rootCids []string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	rcids := make([]boson.Address, 0, len(rootCids))
	for _, rootCid := range rootCids {
		rcid, err := boson.ParseHexAddress(rootCid)
		if err != nil {
			return nil, err
		}
		rcids = append(rcids, rcid)
	}
	var mutex sync.Mutex
	bitMap := make(map[string]BitVectorInfo)
	c, unsub, _ := a.ci.SubscribeDownloadProgress(rcids)

	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				bit := data.(BitVectorInfo)
				mutex.Lock()
				bitMap[bit.RootCid.String()] = bit
				mutex.Unlock()
			case <-ticker.C:
				mutex.Lock()
				bitList := make([]BitVectorInfo, 0, len(bitMap))
				for rootCid, bit := range bitMap {
					bitList = append(bitList, bit)
					delete(bitMap, rootCid)
				}
				mutex.Unlock()
				if len(bitList) > 0 {
					_ = notifier.Notify(sub.ID, bitList)
				}
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()

	return sub, nil
}

func (a *apiService) RetrievalProgress(ctx context.Context, rootCid string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	cid, err := boson.ParseHexAddress(rootCid)
	if err != nil {
		return nil, err
	}
	c, unsub, _ := a.ci.SubscribeRetrievalProgress(cid)
	var mutex sync.Mutex
	bitMap := make(map[string]BitVectorInfo)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				bit := data.(BitVectorInfo)
				mutex.Lock()
				bitMap[bit.Overlay.String()] = bit
				mutex.Unlock()
			case <-ticker.C:
				mutex.Lock()
				bitList := make([]BitVectorInfo, 0, len(bitMap))
				for overlay, bit := range bitMap {
					bitList = append(bitList, bit)
					delete(bitMap, overlay)
				}
				mutex.Unlock()
				if len(bitList) > 0 {
					_ = notifier.Notify(sub.ID, bitList)
				}
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}

func (a *apiService) RootCidStatus(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	c, unsub := a.ci.SubscribeRootCidStatus()
	var mutex sync.Mutex
	rootCidMap := make(map[string]RootCidStatusEven)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				root := data.(RootCidStatusEven)
				mutex.Lock()
				rootCidMap[root.RootCid.String()] = root
				mutex.Unlock()
			case <-ticker.C:
				mutex.Lock()
				rootList := make([]RootCidStatusEven, 0, len(rootCidMap))
				for overlay, bit := range rootCidMap {
					rootList = append(rootList, bit)
					delete(rootCidMap, overlay)
				}
				mutex.Unlock()
				if len(rootList) > 0 {
					_ = notifier.Notify(sub.ID, rootList)
				}
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
