package subscribe

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

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

func AddMetrics(notifier *rpc.Notifier, sub *rpc.Subscription, metrics []prometheus.Metric) {
	logging.Tracef("%s metrics subscribe success", sub.ID)

	notify(notifier, sub, metrics)

	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			notify(notifier, sub, metrics)
		case e := <-sub.Err():
			if e == nil {
				logging.Debugf("%s metrics quit unsubscribe", sub.ID)
			} else {
				logging.Warningf("%s metrics quit %s", sub.ID, e)
			}
			_, _ = cache.Remove(ctx, sub.ID)
			return
		}
	}
}

func notify(notifier *rpc.Notifier, sub *rpc.Subscription, metrics []prometheus.Metric) {
	out := make(map[string]interface{}, len(metrics))
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
	err = notifier.Notify(sub.ID, out)
	if err != nil {
		logging.Errorf("%s metrics notify %s", sub.ID, err)
	} else {
		logging.Tracef("%s metrics notify success", sub.ID)
	}
}
