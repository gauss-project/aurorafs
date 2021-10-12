package routetab

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
)

var (
	NewPendCallResTab = newPendCallResTab
	NewRouteTable     = newRouteTable
)

func (t *Table) TableClean() {
	t.items = make(map[common.Hash]Route)
	t.signed = make(map[common.Hash]*Path)
}

func (s *Service) SetStreamer(recorder *streamtest.Recorder) {
	s.config.Stream = recorder
}