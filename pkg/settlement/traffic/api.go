package traffic

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"sync"
	"time"
)

func (s *Service) API() rpc.API {
	return rpc.API{
		Namespace: "traffic",
		Version:   "1.0",
		Service:   &apiService{s: s},
		Public:    true,
	}
}

type apiService struct {
	s *Service
}

func (a *apiService) Header(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	c, unsub := a.s.SubscribeHeader()
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		var (
			send interface{}
			ok   bool
		)

		for {
			select {
			case data := <-c:
				ok = true
				send = data
			case <-ticker.C:
				if ok {
					_ = notifier.Notify(sub.ID, send)
					ok = false
				}
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}

func (a *apiService) TrafficCheque(ctx context.Context, overlays []string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()
	overs := make([]common.Address, 0, len(overlays))
	for _, overlay := range overlays {
		over, err := boson.ParseHexAddress(overlay)
		if err != nil {
			return nil, err
		}
		recipient, known := a.s.addressBook.Beneficiary(over)
		if !known {
			continue
		}
		overs = append(overs, recipient)
	}
	var mutex sync.Mutex
	chequeMap := make(map[string]interface{})
	c, unsub := a.s.SubscribeTrafficCheque(overs)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				cheque := data.(TrafficCheque)
				mutex.Lock()
				chequeMap[cheque.Peer.String()] = cheque
				mutex.Unlock()
			case <-ticker.C:
				if len(chequeMap) == 0 {
					continue
				}
				mutex.Lock()
				cheques := make([]interface{}, 0, len(chequeMap))
				for overlay, bit := range chequeMap {
					cheques = append(cheques, bit)
					delete(chequeMap, overlay)
				}
				mutex.Unlock()
				_ = notifier.Notify(sub.ID, cheques)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
func (a *apiService) CashOut(ctx context.Context, overlays []string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()
	overs := make([]boson.Address, 0, len(overlays))
	for _, overlay := range overlays {
		over, err := boson.ParseHexAddress(overlay)
		if err != nil {
			return nil, err
		}
		overs = append(overs, over)
	}
	var mutex sync.Mutex
	cashOutMap := make(map[string]interface{})
	c, unsub := a.s.SubscribeCashOut(overs)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				cashOut := data.(CashOutStatus)
				mutex.Lock()
				cashOutMap[cashOut.Overlay.String()] = data
				mutex.Unlock()
			case <-ticker.C:
				if len(cashOutMap) == 0 {
					continue
				}
				mutex.Lock()
				cashOuts := make([]interface{}, 0, len(cashOutMap))
				for overlay, cashOut := range cashOutMap {
					cashOuts = append(cashOuts, cashOut)
					delete(cashOutMap, overlay)
				}
				mutex.Unlock()
				_ = notifier.Notify(sub.ID, cashOuts)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
