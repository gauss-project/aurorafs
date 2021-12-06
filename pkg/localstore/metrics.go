package localstore

import (
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	TotalTimeGCLock         prometheus.Counter
	TotalTimeGCFirstItem    prometheus.Counter
	TotalTimeCollectGarbage prometheus.Counter
	TotalTimeGet            prometheus.Counter
	TotalTimeUpdateGC       prometheus.Counter
	TotalTimeGetMulti       prometheus.Counter
	TotalTimeHas            prometheus.Counter
	TotalTimeHasMulti       prometheus.Counter
	TotalTimePut            prometheus.Counter
	TotalTimeSet            prometheus.Counter

	GCCounter          prometheus.Counter
	GCErrorCounter     prometheus.Counter
	GCCollectedCounter prometheus.Counter
	GCCommittedCounter prometheus.Counter
	GCUpdate           prometheus.Counter
	GCUpdateError      prometheus.Counter

	ModeGet             prometheus.Counter
	ModeGetFailure      prometheus.Counter
	ModeGetMulti        prometheus.Counter
	ModeGetMultiChunks  prometheus.Counter
	ModeGetMultiFailure prometheus.Counter
	ModePut             prometheus.Counter
	ModePutFailure      prometheus.Counter
	ModeSet             prometheus.Counter
	ModeSetFailure      prometheus.Counter
	ModeHas             prometheus.Counter
	ModeHasFailure      prometheus.Counter
	ModeHasMulti        prometheus.Counter
	ModeHasMultiFailure prometheus.Counter

	GCSize                  prometheus.Gauge
	GCStoreTimeStamps       prometheus.Gauge
	GCStoreAccessTimeStamps prometheus.Gauge
}

func newMetrics() metrics {
	subsystem := "localstore"

	return metrics{
		TotalTimeGCLock: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_lock_time",
			Help:      "Total time under lock in gc.",
		}),
		TotalTimeGCFirstItem: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_first_item_time",
			Help:      "Total time taken till first item in gc comes out of gcIndex iterator.",
		}),
		TotalTimeCollectGarbage: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_time",
			Help:      "Total time taken to collect garbage.",
		}),
		TotalTimeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "get_chunk_time",
			Help:      "Total time taken to get chunk from DB.",
		}),
		TotalTimeUpdateGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "in_gc_time",
			Help:      "Total time taken to in gc.",
		}),
		TotalTimeGetMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "get_multi_time",
			Help:      "Total time taken to get multiple chunks from DB.",
		}),
		TotalTimeHas: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "has_time",
			Help:      "Total time taken to check if the key is present in DB.",
		}),
		TotalTimeHasMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "has_multi_time",
			Help:      "Total time taken to check if multiple keys are present in DB.",
		}),
		TotalTimePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "put_time",
			Help:      "Total time taken to put a chunk in DB.",
		}),
		TotalTimeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "set_time",
			Help:      "Total time taken to set chunk in DB.",
		}),
		GCCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_count",
			Help:      "Number of times the GC operation is done.",
		}),
		GCErrorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_fail_count",
			Help:      "Number of times the GC operation failed.",
		}),
		GCCollectedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_collected_count",
			Help:      "Number of times the GC_COLLECTED operation is done.",
		}),
		GCCommittedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_committed_count",
			Help:      "Number of gc items to commit.",
		}),
		GCUpdate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_update_count",
			Help:      "Number of times the gc is updated.",
		}),
		GCUpdateError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_update_error_count",
			Help:      "Number of times the gc update had error.",
		}),

		ModeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_count",
			Help:      "Number of times MODE_GET is invoked.",
		}),
		ModeGetFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_failure_count",
			Help:      "Number of times MODE_GET invocation failed.",
		}),
		ModeGetMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_multi_count",
			Help:      "Number of times MODE_MULTI_GET is invoked.",
		}),
		ModeGetMultiChunks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_multi_chunks_count",
			Help:      "Number of chunks requested through MODE_MULTI_GET.",
		}),
		ModeGetMultiFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_multi_failure_count",
			Help:      "Number of times MODE_GET invocation failed.",
		}),
		ModePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_put_count",
			Help:      "Number of times MODE_PUT is invoked.",
		}),
		ModePutFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_put_failure_count",
			Help:      "Number of times MODE_PUT invocation failed.",
		}),
		ModeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_set_count",
			Help:      "Number of times MODE_SET is invoked.",
		}),
		ModeSetFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_set_failure_count",
			Help:      "Number of times MODE_SET invocation failed.",
		}),
		ModeHas: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_count",
			Help:      "Number of times MODE_HAS is invoked.",
		}),
		ModeHasFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_failure_count",
			Help:      "Number of times MODE_HAS invocation failed.",
		}),
		ModeHasMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_multi_count",
			Help:      "Number of times MODE_HAS_MULTI is invoked.",
		}),
		ModeHasMultiFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_multi_failure_count",
			Help:      "Number of times MODE_HAS_MULTI invocation failed.",
		}),

		GCSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_size",
			Help:      "Number of elements in Garbage collection index.",
		}),
		GCStoreTimeStamps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_time_stamp",
			Help:      "Storage timestamp in Garbage collection iteration.",
		}),
		GCStoreAccessTimeStamps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_access_time_stamp",
			Help:      "Access timestamp in Garbage collection iteration.",
		}),
	}
}

func (db *DB) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(db.metrics)
}
