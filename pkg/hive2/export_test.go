package hive2

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
)

var LookupDistances = lookupDistances
var NewLookup = newLookup

func (l *lookup) Run() {
	l.run()
}

func (l *lookup) Query(node boson.Address) {
	l.query(node)
}

func (l *lookup) GetTotal() int32 {
	return l.total
}

func (l *lookup) GetFind() []boson.Address {
	return l.find
}

func (s *Service) SetStreamer(recorder *streamtest.Recorder) {
	s.streamer = recorder
}
