package subscribe

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gogf/gf/v2/os/gcache"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var (
	cache = gcache.New()
	ctx   = gctx.New()
)

func CacheRemove(ctx context.Context, keys ...interface{}) {
	_, _ = cache.Remove(ctx, keys)
}

func ProcessMetricMsg(sub *rpc.Subscription, metrics []prometheus.Metric) (send bool, out map[string]interface{}) {
	send = false
	out = make(map[string]interface{}, len(metrics))
	for _, mt := range metrics {
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
		logging.Errorf("%s metrics marshal %s", sub.ID, err)
		return
	}
	val, _ := cache.Get(ctx, sub.ID)
	if val != nil && bytes.Equal(bs, val.Bytes()) {
		return
	}
	_ = cache.Set(ctx, sub.ID, bs, 0)
	send = true
	return
}
