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
)

// discover is a forever loop that manages the find to new peers
func (s *Service) discover() {
	defer s.wg.Done()
	defer s.logger.Debugf("hive2: discover loop exited")

	tick := time.NewTicker(time.Minute * 30)
	tickFirst := time.NewTicker(time.Second * 30)
	runWorkC := make(chan struct{}, 1)
	for {
		select {
		case <-s.quit:
			return
		case <-tickFirst.C:
			// wait for kademlia have A certain amount of saturation
			// when boot maybe have many peer in address book
			runWorkC <- struct{}{}
			tickFirst.Stop()
		case <-tick.C:
			runWorkC <- struct{}{}
		case <-runWorkC:
			s.discoverWork()
		}
	}
}

func (s *Service) discoverWork() {
	start := time.Now()
	s.logger.Debugf("hive2 discover start...")
	defer s.logger.Debugf("hive2 discover took %s to finish", time.Since(start))
	newLookup(s.config.Base, s).run()
	for i := 0; i < 3; i++ {
		dest := test.RandomAddress()
		newLookup(dest, s).run()
	}
}

type lookup struct {
	h2       *Service
	target   boson.Address
	total    atomic.Int32
	stop     chan bool
	find     []boson.Address
	asked    map[string]bool
}

func newLookup(target boson.Address, s *Service) *lookup {
	return &lookup{
		target:   target,
		h2:       s,
		stop:     make(chan bool, 1),
		asked:    make(map[string]bool),
	}
}

func (l *lookup) run() {
	l.findNode()
	for {
		select {
		case <-l.stop:
			return
		default:
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

func (l *lookup) findNode() {
	peers, err := l.h2.config.Kad.ClosestPeers(l.target, lookupNeighborPeerLimit)
	if err != nil {
		l.h2.logger.Warningf("ClosestPeers %s", err)
		return
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
			select {
			case addr := <-ch:
				if addr.IsZero() {
					return
				}
				if l.asked[addr.String()] {
					break
				}
				if l.total.Inc() >= findNodePeerLimit {
					l.stop <- true
					return
				}
				l.asked[addr.String()] = true
				l.find = append(l.find, addr)
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
