package retrieval

import (
	"context"

	mts "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/gauss-project/aurorafs/pkg/rpc"
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

	mts.AddSubscribe(notifier, sub, []prometheus.Metric{
		a.s.metrics.TotalRetrieved,
		a.s.metrics.TotalTransferred,
	})
	return sub, nil
}