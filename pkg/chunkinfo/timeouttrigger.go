package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"strings"
	"sync"
	"time"
)

const (
	TimeOut = 1000
	Time    = 3
)

// timeoutTrigger
type timeoutTrigger struct {
	sync.RWMutex
	// rootCid_overlay : Timestamp
	trigger map[string]int64
}

func newTimeoutTrigger() *timeoutTrigger {
	return &timeoutTrigger{trigger: make(map[string]int64)}
}

// updateTimeOutTrigger
func (tt *timeoutTrigger) updateTimeOutTrigger(rootCid, overlay []byte) {
	tt.Lock()
	tt.Unlock()
	key := string(rootCid) + "_" + string(overlay)
	tt.trigger[key] = time.Now().Unix()
}

// removeTimeOutTrigger
func (tt *timeoutTrigger) removeTimeOutTrigger(rootCid, overlay boson.Address) {
	tt.Lock()
	tt.Unlock()
	key := rootCid.ByteString() + "_" + overlay.ByteString()
	delete(tt.trigger, key)
}

// getTimeOutRootCidAndNode
func (tt *timeoutTrigger) getTimeOutRootCidAndNode() ([]byte, []byte) {
	for k, t := range tt.trigger {
		if t+TimeOut <= time.Now().Unix() {
			arr := strings.Split(k, "_")
			return []byte(arr[0]), []byte(arr[1])
		}
	}
	return nil, nil
}

// triggerTimeOut
func (ci *ChunkInfo) triggerTimeOut() {
	timeTrigger := ci.t
	select {
	case <-timeTrigger.C:
		rootCid, overlay := ci.tt.getTimeOutRootCidAndNode()
		if rootCid != nil {
			q := ci.getQueue(string(rootCid))
			// 超时从正在执行放入到未执行
			q.popNode(Pulling, overlay)
			q.push(UnPull, overlay)
		}
	}
}
