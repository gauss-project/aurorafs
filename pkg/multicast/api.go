package multicast

import (
	"context"
	"time"

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

func (a *apiService) Peers(ctx context.Context, name string) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	gid, err := boson.ParseHexAddress(name)
	if err != nil {
		gid = GenerateGID(name)
	}
	err = a.s.subscribeGroupPeers(gid, &PeersSubClient{
		notify:       notifier,
		sub:          sub,
		lastPushTime: time.Now(),
	})
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Reply to the group message to give the session ID
func (a *apiService) Reply(sessionID string, data []byte) error {
	return a.s.replyGroupMessage(sessionID, data)
}
