package kademlia

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"time"
)

// discover is a forever loop that manages the find to new peers
func (k *Kad) discover() {
	defer k.wg.Done()
	if !k.discovery.IsStart() {
		return
	}
	defer k.logger.Debugf("kademlia discover loop exited")

	worker := func() {
		start := time.Now()
		k.logger.Debugf("kademlia discover start...")
		defer k.logger.Debugf("kademlia discover end. %d ms", time.Since(start).Milliseconds())
		stop, jumpNext, _ := k.startFindNode(k.base, 0)
		if stop {
			return
		}
		if jumpNext {
			for i := 0; i < 3; i++ {
				dest := test.RandomAddress()
				stop, _, _ = k.startFindNode(dest, 0)
				if stop {
					return
				}
			}
		}
	}

	tick := time.NewTicker(time.Minute * 30)
	firstRun := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-k.halt:
			return
		case <-k.quit:
			return
		case <-firstRun.C:
			worker()
			firstRun.Stop()
		case <-tick.C:
			worker()
		}
	}
}

func (k *Kad) startFindNode(target boson.Address, total int) (stop bool, jumpNext bool, count int) {
	var ch chan boson.Address
	ch, stop, jumpNext, count = k.lookup(target, total)
	if stop || jumpNext {
		return
	}
	for addr := <-ch; !addr.IsZero(); {
		stop, jumpNext, count = k.startFindNode(addr, count)
		if stop || jumpNext {
			return
		}
	}
	return
}

func (k *Kad) lookup(target boson.Address, total int) (ch chan boson.Address, stop bool, jumpNext bool, count int) {
	stop = true
	var lookupBin []uint8
	for i := uint8(0); i < boson.MaxBins; i++ {
		if saturate, _ := k.saturationFunc(i, k.knownPeers, k.connectedPeers); saturate {
			continue
		}
		stop = false
		lookupBin = append(lookupBin, i)
	}
	if !stop {
		// need discover
		peers, err := k.ClosestPeers(target, lookupNeighborPeerLimit)
		if err != nil {
			k.logger.Warningf("ClosestPeers %s", err)
			return
		}
		for _, dest := range peers {
			pos := lookupDistances(target, dest, lookupPoLimit, lookupBin)
			if len(pos) > 0 {
				var cnt int
				ch, cnt, _ = k.discovery.DoFindNode(context.Background(), dest, pos, findNodePeerLimit)
				count = total + cnt
				if count >= findNodePeerLimit {
					jumpNext = true
					return
				}
			}
		}
		jumpNext = true
	}
	return
}

// e.g. lookupRequestLimit=3
// for a target with Proximity(target, dest) = 5 the result is [5, 6, 4].
func lookupDistances(target, dest boson.Address, lookupRequestLimit int, pick []uint8) (pos []int32) {
	po := boson.Proximity(target.Bytes(), dest.Bytes())
	pos = append(pos, int32(po))
	for i := uint8(1); len(pos) < lookupRequestLimit; i++ {
		if po+i <= boson.MaxPO && inArray(po+i, pick) {
			pos = append(pos, int32(po+i))
		}
		if po-i > 0 && inArray(po-i, pick) {
			pos = append(pos, int32(po-i))
		}
	}
	return pos
}

func inArray(i uint8, pick []uint8) bool {
	for _, v := range pick {
		if v == i {
			return true
		}
	}
	return false
}
