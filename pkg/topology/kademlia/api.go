package kademlia

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
)

func (k *Kad) API(light *lightnode.Container, boot *bootnode.Container) rpc.API {
	return rpc.API{
		Namespace: "p2p",
		Version:   "1.0",
		Service:   &apiService{kad: k, lightNodes: light, bootNodes: boot},
		Public:    true,
	}
}

type apiService struct {
	kad        *Kad
	lightNodes *lightnode.Container
	bootNodes  *bootnode.Container
}

type Connected struct {
	FullNodes  int  `json:"full_nodes"`
	LightNodes uint `json:"light_nodes"`
	BootNodes  uint `json:"boot_nodes"`
}

type KadInfo struct {
	Depth      uint8     `json:"depth"`
	Population int       `json:"population"`
	Connected  Connected `json:"connected"`
}

func (a *apiService) KadInfo(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	ch, unsub := a.kad.SubscribePeersChange()

	go func() {
		var (
			lastTime    = time.Now()
			minInterval = time.Second
			waitAfter   bool
		)
		resFunc := func() {
			params := a.kad.Snapshot()
			params.LightNodes = a.lightNodes.PeerInfo()
			params.BootNodes = a.bootNodes.PeerInfo()
			result := &KadInfo{
				Depth:      params.Depth,
				Population: params.Population,
				Connected: Connected{
					FullNodes:  params.Connected,
					LightNodes: params.LightNodes.BinConnected,
					BootNodes:  params.BootNodes.BinConnected,
				},
			}
			_ = notifier.Notify(sub.ID, result)
			lastTime = time.Now()
		}
		for {
			select {
			case <-ch:
				if waitAfter {
					break
				}
				ms := time.Since(lastTime)
				if ms >= minInterval {
					resFunc()
					break
				}
				go func() {
					waitAfter = true
					<-time.After(minInterval - ms)
					resFunc()
					waitAfter = false
				}()
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}

func (a *apiService) PeerState(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	ch, unsub := a.kad.SubscribePeerState()

	go func() {
		for {
			select {
			case p := <-ch:
				_ = notifier.Notify(sub.ID, p)
			case <-sub.Err():
				unsub()
				return
			}
		}
	}()
	return sub, nil
}
