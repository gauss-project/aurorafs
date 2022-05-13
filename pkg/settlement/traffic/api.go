package traffic

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
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
	a.s.SubscribeHeader(notifier, sub)
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
	a.s.SubscribeTrafficCheque(notifier, sub, overs)
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
	a.s.SubscribeCashOut(notifier, sub, overs)
	return sub, nil
}
