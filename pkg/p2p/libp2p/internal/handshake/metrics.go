package handshake

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// metrics groups handshake related prometheus counters.
type metrics struct {
	SynRx          prometheus.Counter
	SynRxFailed    prometheus.Counter
	SynAckTx       prometheus.Counter
	SynAckTxFailed prometheus.Counter
	AckRx          prometheus.Counter
	AckRxFailed    prometheus.Counter
}

// newMetrics is a convenient constructor for creating new metrics.
func newMetrics() metrics {
	const subsystem = "handshake"

	return metrics{
		SynRx: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "syn_rx",
			Help:      "The number of syn messages that were successfully read.",
		}),
		SynRxFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "syn_rx_failed",
			Help:      "The number of syn messages that were unsuccessfully read.",
		}),
		SynAckTx: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "syn_ack_tx",
			Help:      "The number of syn-ack messages that were successfully written.",
		}),
		SynAckTxFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "syn_ack_tx_failed",
			Help:      "The number of syn-ack messages that were unsuccessfully written.",
		}),
		AckRx: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "ack_rx",
			Help:      "The number of ack messages that were successfully read.",
		}),
		AckRxFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "ack_rx_failed",
			Help:      "The number of ack messages that were unsuccessfully read.",
		}),
	}
}

// Metrics returns set of prometheus collectors.
func (s *Service) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
