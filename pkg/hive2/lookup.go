package hive2

import (
	"context"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/topology"
)

const (
	lookupRequestLimit      = 3
	lookupNeighborPeerLimit = 3
	findNodePeerLimit       = 16
	findWorkInterval        = time.Minute * 30
)

// discover is a forever loop that manages the find to new peers
func (s *Service) discover() {
	defer s.logger.Debugf("hive2: discover loop exited")

	tick := time.NewTicker(findWorkInterval)
	for {
		select {
		case <-s.quit:
			return
		case <-tick.C:
			s.NotifyDiscoverWork()
		case peers := <-s.findWorkC:
			s.discoverWork(peers...)
		}
	}
}

func (s *Service) discoverWork(peers ...boson.Address) {
	start := time.Now()
	if len(peers) > 0 {
		s.logger.Infof("hive2: discover start to %s", peers[0])
	} else {
		s.logger.Infof("hive2: discover start to default")
	}
	s.logger.Tracef("hive2: discover run self %s", s.config.Base)
	newLookup(s.config.Base, s).run(peers...)
	for i := 0; i < 3; i++ {
		dest := test.RandomAddress()
		s.logger.Tracef("hive2: discover run random %s", dest)
		newLookup(dest, s).run(peers...)
	}
	s.logger.Infof("hive2: discover end for took %s", time.Since(start))
}

// NotifyDiscoverWork notifies hive2 discover work.
func (s *Service) NotifyDiscoverWork(peers ...boson.Address) {
	if s.IsStart() {
		select {
		case s.findWorkC <- peers:
		default:
		}
	}
}

type lookup struct {
	h2     *Service
	target boson.Address
	total  int32
	find   []boson.Address
	asked  map[string]bool
}

func newLookup(target boson.Address, s *Service) *lookup {
	return &lookup{
		target: target,
		h2:     s,
		asked:  make(map[string]bool),
	}
}

func (l *lookup) run(forward ...boson.Address) {
	l.findNode(forward...)
	for {
		<-time.After(time.Millisecond * 100)
		if l.total >= findNodePeerLimit || len(l.find) == 0 {
			return
		}
		list := l.find
		l.find = make([]boson.Address, 0)
		l.queryLimit(list)
	}
}

func (l *lookup) findNode(forward ...boson.Address) {
	var peers []boson.Address
	if len(forward) > 0 {
		peers = forward
	} else {
		var err error
		peers, err = l.h2.config.Kad.ClosestPeers(l.target, lookupNeighborPeerLimit, topology.Filter{Reachable: false})
		if err != nil {
			l.h2.logger.Warningf("ClosestPeers %s", err)
			return
		}
	}
	l.queryLimit(peers)
}

func (l *lookup) queryLimit(peers []boson.Address) {
	for _, node := range peers {
		l.query(node)
		if l.total >= findNodePeerLimit {
			return
		}
	}
}

func (l *lookup) query(node boson.Address) {
	pos := lookupDistances(l.target, node)
	var posReq []int32
	for _, v := range pos {
		if !l.h2.config.Kad.IsSaturated(uint8(v)) {
			posReq = append(posReq, v)
		}
	}
	l.h2.logger.Tracef("hive2: query target=%s pos=%v limit=%d node=%s", l.target, posReq, findNodePeerLimit-l.total, node)
	if len(posReq) > 0 {
		ch, err := l.h2.DoFindNode(context.Background(), l.target, node, posReq, findNodePeerLimit-l.total)
		if err != nil {
			return
		}
		for {
			addr := <-ch
			if addr.IsZero() {
				l.h2.logger.Tracef("hive2: query received peers %d", l.total)
				return
			}
			if _, ok := l.asked[addr.String()]; ok {
				l.h2.logger.Tracef("hive2: query asked %s", addr)
				continue
			}
			l.asked[addr.String()] = true
			l.find = append(l.find, addr)
			l.total++

			if l.total >= findNodePeerLimit {
				return
			}
		}
	}
}

// for a target with Proximity(target, dest) = 5 the result is [5, 6, 4].
// skip saturation po
func lookupDistances(target, dest boson.Address) (pos []int32) {
	po := boson.Proximity(target.Bytes(), dest.Bytes())
	pos = append(pos, int32(po))
	for i := uint8(1); len(pos) < lookupRequestLimit; i++ {
		if po+i < boson.MaxBins {
			pos = append(pos, int32(po+i))
		}
		if int(po)-int(i) >= 0 {
			pos = append(pos, int32(po-i))
		}
	}
	return pos
}

func inArray(bin uint8, pos []int32) bool {
	for _, v := range pos {
		if bin == uint8(v) {
			return true
		}
	}
	return false
}
