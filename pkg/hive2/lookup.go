package hive2

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"go.uber.org/atomic"
	"time"
)

const (
	lookupRequestLimit      = 3
	lookupNeighborPeerLimit = 3
	findNodePeerLimit       = 16
	findWorkInterval        = time.Minute * 30
)

// discover is a forever loop that manages the find to new peers
func (s *Service) discover() {
	defer s.wg.Done()
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
	s.logger.Debugf("hive2 discover start...")
	defer s.logger.Debugf("hive2 discover took %s to finish", time.Since(start))
	newLookup(s.config.Base, s).run(peers...)
	for i := 0; i < 3; i++ {
		dest := test.RandomAddress()
		newLookup(dest, s).run(peers...)
	}
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
	total  atomic.Int32
	stop   chan bool
	find   []boson.Address
	asked  map[string]bool
}

func newLookup(target boson.Address, s *Service) *lookup {
	return &lookup{
		target: target,
		h2:     s,
		stop:   make(chan bool, 1),
		asked:  make(map[string]bool),
	}
}

func (l *lookup) run(forward ...boson.Address) {
	l.findNode(forward...)
	for {
		select {
		case <-l.stop:
			return
		default:
			<-time.After(time.Millisecond * 50)
			if len(l.find) == 0 {
				return
			}
			list := l.find
			l.find = make([]boson.Address, 0)
			for _, node := range list {
				l.query(node)
			}
		}
	}
}

func (l *lookup) findNode(forward ...boson.Address) {
	var peers []boson.Address
	if len(forward) > 0 {
		peers = forward
	} else {
		var err error
		peers, err = l.h2.config.Kad.ClosestPeers(l.target, lookupNeighborPeerLimit)
		if err != nil {
			l.h2.logger.Warningf("ClosestPeers %s", err)
			return
		}
	}
	for _, node := range peers {
		l.query(node)
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
	if len(posReq) > 0 {
		ch, err := l.h2.DoFindNode(context.Background(), l.target, node, posReq, findNodePeerLimit-l.total.Load())
		if err != nil {
			return
		}
		for {
			addr := <-ch
			if addr.IsZero() {
				return
			}
			if _, ok := l.asked[addr.String()]; ok {
				continue
			}
			l.asked[addr.String()] = true
			l.find = append(l.find, addr)

			if l.total.Inc() >= findNodePeerLimit {
				l.stop <- true
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
