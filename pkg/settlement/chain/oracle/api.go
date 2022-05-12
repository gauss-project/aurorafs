package oracle

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
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

	a.ora.SubscribeRegisterStatus(notifier, sub, rcids)
	return sub, nil
}
