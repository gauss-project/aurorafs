package metrics

import (
	"bytes"
	"container/list"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gogf/gf/v2/os/gcache"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Namespace is prefixed before every metric. If it is changed, it must be done
// before any metrics collector is registered.
var Namespace = "aurora"

type Collector interface {
	Metrics() []prometheus.Collector
}

func PrometheusCollectorsFromFields(i interface{}) (cs []prometheus.Collector) {
	v := reflect.Indirect(reflect.ValueOf(i))
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).CanInterface() {
			continue
		}
		if u, ok := v.Field(i).Interface().(prometheus.Collector); ok {
			cs = append(cs, u)
		}
	}
	return cs
}

var (
	cache  = gcache.New()
	ctx    = gctx.New()
	manage = list.New()
)

type subClient struct {
	notifier *rpc.Notifier
	sub      *rpc.Subscription
	metrics  []prometheus.Metric
}

func init() {
	go func() {
		t := time.NewTicker(time.Second * 5)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				for item := manage.Front(); item != nil; item = item.Next() {
					v := item.Value.(*subClient)
					select {
					case <-v.sub.Err():
						manage.Remove(item)
						continue
					default:
					}
					out := make(map[string]interface{}, len(v.metrics))
					for _, mt := range v.metrics {
						data := &dto.Metric{}
						_ = mt.Write(data)
						var val interface{}
						switch {
						case data.Counter != nil:
							val = data.Counter.Value
						case data.Gauge != nil:
							val = data.Gauge.Value
						case data.Histogram != nil:
							val = data.Histogram
						}
						name := mt.Desc().String()
						ns := strings.Split(name, "\"")
						out[ns[1]] = val
					}
					bs, err := json.Marshal(out)
					if err != nil {
						break
					}
					val, _ := cache.Get(ctx, v.sub.ID)
					if val != nil && bytes.Equal(bs, val.Bytes()) {
						break
					}
					_ = cache.Set(ctx, v.sub.ID, bs, 0)
					_ = v.notifier.Notify(v.sub.ID, out)
				}
			}
		}
	}()
}

func AddSubscribe(notifier *rpc.Notifier, sub *rpc.Subscription, metrics []prometheus.Metric) {
	manage.PushFront(&subClient{
		notifier: notifier,
		sub:      sub,
		metrics:  metrics,
	})
}
