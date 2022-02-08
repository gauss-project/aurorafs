package routetab

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"sync"
)

var (
	NewPendCallResTab = newPendCallResTab
	NewRouteTable     = newRouteTable
	GetPendingReqKey  = getPendingReqKey
)

func (pend *pendCallResTab) ReqLogRange(f func(key, value interface{}) bool) {
	pend.reqList.Range(f)
}

func (t *Table) TableClean() {
	t.routes = make(map[common.Hash][]TargetRoute)
	t.paths = sync.Map{}
}

func (s *Service) SetStreamer(recorder *streamtest.Recorder) {
	s.stream = recorder
}
