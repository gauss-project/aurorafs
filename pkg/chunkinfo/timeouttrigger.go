package chunkinfo

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"strings"
	"sync"
	"time"
)

const (
	TimeOut = 30
	Time    = 5
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
	defer tt.Unlock()
	key := boson.NewAddress(rootCid).String() + "_" + boson.NewAddress(overlay).String()
	tt.trigger[key] = time.Now().Unix()
}

// removeTimeOutTrigger
func (tt *timeoutTrigger) removeTimeOutTrigger(rootCid, overlay boson.Address) {
	tt.Lock()
	defer tt.Unlock()
	key := rootCid.String() + "_" + overlay.String()
	delete(tt.trigger, key)
}

// getTimeOutRootCidAndNode
func (tt *timeoutTrigger) getTimeOutRootCidAndNode() (boson.Address, boson.Address) {
	tt.RLock()
	defer tt.RUnlock()
	for k, t := range tt.trigger {
		if t+TimeOut <= time.Now().Unix() {
			arr := strings.Split(k, "_")
			return boson.MustParseHexAddress(arr[0]), boson.MustParseHexAddress(arr[1])
		}
	}
	return boson.ZeroAddress, boson.ZeroAddress
}

// triggerTimeOut
func (ci *ChunkInfo) triggerTimeOut() {

	t := time.NewTicker(Time * time.Second)
	go func() {
		for {
			<-t.C
			rootCid, overlay := ci.timeoutTrigger.getTimeOutRootCidAndNode()
			if !rootCid.Equal(boson.ZeroAddress) {
				q := ci.getQueue(rootCid.String())
				if q != nil {
					msgChan, ok := ci.syncMsg.Load(rootCid.String())
					if ok {
						msgChan.(chan bool) <- false
					}
					q.popNode(Pulling, overlay.Bytes())
					q.push(UnPull, overlay.Bytes())
				} else {
					ci.timeoutTrigger.removeTimeOutTrigger(rootCid, overlay)
				}
			}
		}
	}()
}
