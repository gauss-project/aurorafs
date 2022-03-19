package oracle

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"time"
)

func (ora *ChainOracle) API() rpc.API {
	return rpc.API{
		Namespace: "oracle",
		Version:   "1.0",
		Service:   &apiService{ora: ora},
		Public:    true,
	}
}

type apiService struct {
	ora *ChainOracle
}

func (a *apiService) RegisterStatus(ctx context.Context, rootCids []string) (*rpc.Subscription, error) {
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

	c, unsub := a.ora.SubscribeRegisterStatus(rcids)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case data := <-c:
				_ = notifier.Notify(sub.ID, data)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
