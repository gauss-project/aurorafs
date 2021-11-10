package chunkinfo

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/gauss-project/aurorafs/pkg/metrics"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection

	ChunkInfoRequestCounter prometheus.Counter
	PyramidRequestCounter   prometheus.Counter
	ChunkInfoTotalErrors    prometheus.Counter
	PyramidTotalErrors      prometheus.Counter
	ChunkInfoRespCounter    prometheus.Counter
	PyramidRespCounter      prometheus.Counter
}

func newMetrics() metrics {
	subsystem := "chunkinfo"

	return metrics{
		ChunkInfoRequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunk_info_request_counter",
			Help:      "Number of requests to chunk info.",
		}),
		PyramidRequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pyramid_request_counter",
			Help:      "Total number of errors while pyramid.",
		}),
		ChunkInfoTotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunk_info_total_errors",
			Help:      "Total number of errors while chunk info.",
		}),
		PyramidTotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pyramid_total_errors",
			Help:      "Total number of errors while pyramid.",
		}),
		ChunkInfoRespCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunk_info_resp_counter",
			Help:      "ChunkInfo successfully returned number.",
		}),
		PyramidRespCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pyramid_resp_counter",
			Help:      "Pyramid successfully returned number.",
		}),
	}
}

func (ci *ChunkInfo) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(ci.metrics)
}
