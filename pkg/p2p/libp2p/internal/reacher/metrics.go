package reacher

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	Pings    prometheus.CounterVec
	PingTime prometheus.HistogramVec
}

func newMetrics() metrics {
	subsystem := "reacher"

	return metrics{
		Pings: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pings",
			Help:      "Ping counter.",
		}, []string{"status"}),
		PingTime: *prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "ping_timer",
			Help:      "Ping timer.",
		}, []string{"status"}),
	}
}

func (s *reacher) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
