package retrieval

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/prometheus/client_golang/prometheus"
)

func (s *Service) API() rpc.API {
	return rpc.API{
		Namespace: "retrieval",
		Version:   "1.0",
		Service:   &apiService{s: s},
		Public:    true,
	}
}

type apiService struct {
	s *Service
}

func (a *apiService) Metrics(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	iNotifier := subscribe.NewNotifier(notifier, sub)
	_ = a.s.subPub.Subscribe(iNotifier, "retrieval", "metrics", "")

	go func() {
		logging.Tracef("%s metrics subscribe success", sub.ID)

		send, out := subscribe.ProcessMetricMsg(sub, []prometheus.Metric{
			a.s.metrics.TotalRetrieved,
			a.s.metrics.TotalTransferred,
		})
		if send {
			err := a.s.subPub.Publish("retrieval", "metrics", "", out)
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
					a.s.metrics.TotalRetrieved,
					a.s.metrics.TotalTransferred,
				})
				if send {
					err := a.s.subPub.Publish("retrieval", "metrics", "", out)
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
