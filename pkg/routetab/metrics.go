package routetab

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection
	FindRouteReqSentCount      prometheus.Counter
	FindRouteRespSentCount     prometheus.Counter
	FindRouteReqReceivedCount  prometheus.Counter
	FindRouteRespReceivedCount prometheus.Counter
	TotalErrors                prometheus.Counter

	TotalOutboundConnections              prometheus.Counter
	TotalOutboundConnectionFailedAttempts prometheus.Counter
}

func newMetrics() metrics {
	subsystem := "routetab"

	return metrics{
		FindRouteReqSentCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "route_req_sent_count",
			Help:      "Number of route requests sent.",
		}),
		FindRouteRespSentCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "route_resp_sent_count",
			Help:      "Number of route responses sent.",
		}),
		FindRouteReqReceivedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "route_req_received_count",
			Help:      "Number of route requests received.",
		}),
		FindRouteRespReceivedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "route_resp_received_count",
			Help:      "Number of route responses received.",
		}),
		TotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "route_errors_total_count",
			Help:      "Number of route errors total.",
		}),
		TotalOutboundConnections: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_outbound_connection",
			Help:      "Total outbound connection made.",
		}),
		TotalOutboundConnectionFailedAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_outbound_connection_failed_attempts",
			Help:      "Total outbound connection failed attempts made.",
		}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
