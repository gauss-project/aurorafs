package multicast

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/rpc"
)

func (s *Service) API() rpc.API {
	return rpc.API{
		Namespace: "group",
		Version:   "1.0",
		Service:   &apiService{s: s},
		Public:    true,
	}
}

type apiService struct {
	s *Service
}

// Message subscribe the group message
func (a *apiService) Message(ctx context.Context, name string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	gid, err := boson.ParseHexAddress(name)
	if err != nil {
		gid = GenerateGID(name)
	}
	ch, unsub, err := a.s.SubscribeGroupMessage(gid)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case data := <-ch:
				_ = notifier.Notify(sub.ID, data)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}

// Multicast subscribe the group multicast message
func (a *apiService) Multicast(ctx context.Context, name string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	gid, err := boson.ParseHexAddress(name)
	if err != nil {
		gid = GenerateGID(name)
	}
	ch, unsub, err := a.s.SubscribeMulticastMsg(gid)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case data := <-ch:
				_ = notifier.Notify(sub.ID, data)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
