package chunkinfo

import (
	"strings"
	"sync"
	"time"
)

const (
	TimeOut = 5
	Time    = 3
)

// timeoutTrigger
type timeoutTrigger struct {
	sync.RWMutex
	// rootCid_node : Timestamp
	trigger map[string]int64
}

// updateTimeOutTrigger
func (tt *timeoutTrigger) updateTimeOutTrigger(rootCid, nodeId string) {
	tt.Lock()
	tt.Unlock()
	key := rootCid + "_" + nodeId
	tt.trigger[key] = time.Now().Unix()
}

// removeTimeOutTrigger
func (tt *timeoutTrigger) removeTimeOutTrigger(rootCid, nodeId string) {
	tt.Lock()
	tt.Unlock()
	key := rootCid + "_" + nodeId
	delete(tt.trigger, key)
}

// getTimeOutRootCidAndNode
func (tt *timeoutTrigger) getTimeOutRootCidAndNode() (string, string) {
	for k, t := range tt.trigger {
		if t+TimeOut <= time.Now().Unix() {
			arr := strings.Split(k, "_")
			return arr[0], arr[1]
		}
	}
	return "", ""
}

// triggerTimeOut
func (ci *ChunkInfo) triggerTimeOut() {
	timeTrigger := ci.t
	select {
	case <-timeTrigger.C:
		rootCid, node := ci.tt.getTimeOutRootCidAndNode()
		if rootCid != "" {
			q := ci.getQueue(rootCid)
			// 超时从正在执行放入到未执行
			q.popNode(Pulling, node)
			q.push(UnPull, node)
		}
	}
}
