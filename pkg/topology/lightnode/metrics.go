package lightnode

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// metrics groups lightnode related prometheus counters.
type metrics struct {
	CurrentlyConnectedPeers    prometheus.Gauge
	CurrentlyDisconnectedPeers prometheus.Gauge
}

// newMetrics is a convenient constructor for creating new metrics.
func newMetrics() metrics {
	const subsystem = "lightnode"

	return metrics{
		CurrentlyConnectedPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "currently_connected_peers",
			Help:      "Number of currently connected peers.",
		}),
		CurrentlyDisconnectedPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "currently_disconnected_peers",
			Help:      "Number of currently disconnected peers.",
		})}
}

// Metrics returns set of prometheus collectors.
func (c *Container) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(c.metrics)
}
