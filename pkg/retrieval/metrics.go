package retrieval

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/gauss-project/aurorafs/pkg/metrics"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection

	RequestCounter        prometheus.Counter
	PeerRequestCounter    prometheus.Counter
	TotalRetrieved        prometheus.Counter
	InvalidChunkRetrieved prometheus.Counter
	TotalErrors           prometheus.Counter
	TotalTransferred      prometheus.Counter
	ChunkTransferredError prometheus.Counter
}

func newMetrics() metrics {
	subsystem := "retrieval"

	return metrics{
		RequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_count",
			Help:      "Number of requests to retrieve chunks.",
		}),
		PeerRequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "peer_request_count",
			Help:      "Number of request to single peer.",
		}),
		TotalRetrieved: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_retrieved",
			Help:      "Total chunks retrieved.",
		}),
		InvalidChunkRetrieved: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "invalid_chunk_retrieved",
			Help:      "Invalid chunk retrieved from peer.",
		}),
		TotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_errors",
			Help:      "Total number of errors while retrieving chunk.",
		}),
		TotalTransferred: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_transferred",
			Help:      "Total  chunk transferred",
		}),
		ChunkTransferredError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunk_transferred_error",
			Help:      "error chunk transferred from peer.",
		}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
