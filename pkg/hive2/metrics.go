package hive2

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	DoFindNode       prometheus.Counter
	DoFindNodePeers  prometheus.Counter
	OnFindNode       prometheus.Counter
	OnFindNodePeers  prometheus.Counter
	UnreachablePeers prometheus.Counter
}

func newMetrics() metrics {
	subsystem := "hive2"

	return metrics{
		DoFindNode: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "do_find_node_count",
			Help:      "Number of peers do find node",
		}),
		DoFindNodePeers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "do_find_node_peers_count",
			Help:      "Number of peers received in peer messages.",
		}),
		OnFindNode: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "on_find_node_count",
			Help:      "Number of peer messages received.",
		}),
		OnFindNodePeers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "on_find_node_peers_count",
			Help:      "Number of peers to be sent.",
		}),
		UnreachablePeers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "unreachable_peers_count",
			Help:      "Number of peers that are unreachable.",
		}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
