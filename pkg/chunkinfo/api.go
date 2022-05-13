package chunkinfo

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/prometheus/client_golang/prometheus"
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

	a.ci.SubscribeDownloadProgress(notifier, sub, rcids)

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

	a.ci.SubscribeRetrievalProgress(notifier, sub, cid)
	return sub, nil
}

func (a *apiService) RootCidStatus(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	a.ci.SubscribeRootCidStatus(notifier, sub)
	return sub, nil
}

func (a *apiService) Metrics(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	iNotifier := subscribe.NewNotifier(notifier, sub)
	_ = a.ci.subPub.Subscribe(iNotifier, "chunkInfo", "metrics", "")

	go func() {
		logging.Tracef("%s metrics subscribe success", sub.ID)

		send, out := subscribe.ProcessMetricMsg(sub, []prometheus.Metric{
			a.ci.metrics.PyramidTotalRetrieved,
			a.ci.metrics.PyramidTotalTransferred,
		})
		if send {
			err := a.ci.subPub.Publish("chunkInfo", "metrics", "", out)
			if err != nil {
				logging.Errorf("%s metrics notify %s", sub.ID, err)
			} else {
				logging.Tracef("%s metrics notify success", sub.ID)
			}
		}

		t := time.NewTicker(time.Second * 5)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				send, out = subscribe.ProcessMetricMsg(sub, []prometheus.Metric{
					a.ci.metrics.PyramidTotalRetrieved,
					a.ci.metrics.PyramidTotalTransferred,
				})
				if send {
					err := a.ci.subPub.Publish("retrieval", "metrics", "", out)
					if err != nil {
						logging.Errorf("%s metrics notify %s", sub.ID, err)
					} else {
						logging.Tracef("%s metrics notify success", sub.ID)
					}
				}
			case e := <-sub.Err():
				if e == nil {
					logging.Debugf("%s metrics quit unsubscribe", sub.ID)
				} else {
					logging.Warningf("%s metrics quit %s", sub.ID, e)
				}
				subscribe.CacheRemove(ctx, sub.ID)
				return
			}
		}
	}()
	return sub, nil
}
