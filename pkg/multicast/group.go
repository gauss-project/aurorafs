package multicast

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
)

type Group struct {
	gid            boson.Address
	connectedPeers *pslice.PSlice
	keepPeers      *pslice.PSlice // Need to maintain the connection with ping
	knownPeers     *pslice.PSlice
	srv            *Service
	option         model.ConfigNodeGroup
	mux            sync.RWMutex

	multicastSub bool
	groupMsgSub  bool

	groupPeersLastSend time.Time     // groupPeersMsg last send time
	groupPeersSending  chan struct{} // whether a goroutine is sending msg. This chan needs to be declared whit "make(chan struct{}, 1)"
}

func (s *Service) newGroup(gid boson.Address, o model.ConfigNodeGroup) *Group {
	if o.KeepConnectedPeers < 0 {
		o.KeepConnectedPeers = 0
	}
	if o.KeepPingPeers < 0 {
		o.KeepPingPeers = 0
	}
	g := &Group{
		gid:                gid,
		connectedPeers:     pslice.New(1, s.self),
		keepPeers:          pslice.New(1, s.self),
		knownPeers:         pslice.New(1, s.self),
		srv:                s,
		option:             o,
		groupPeersLastSend: time.Now(),
		groupPeersSending:  make(chan struct{}, 1),
	}
	s.groups.Store(gid.String(), g)
	return g
}

func (s *Service) getAllProtectPeers() (peers []boson.Address) {
	s.groups.Range(func(key, value interface{}) bool {
		g := value.(*Group)
		if g.option.GType == model.GTypeJoin || g.option.GType == model.GTypeObserve {
			peers = append(peers, g.connectedPeers.BinPeers(0)...)
			peers = append(peers, g.keepPeers.BinPeers(0)...)
		}
		return true
	})
	return peers
}

func (s *Service) getGroupAll() (old []*Group) {
	s.groups.Range(func(key, value interface{}) bool {
		old = append(old, value.(*Group))
		return true
	})
	return
}

func (s *Service) getGroup(gid boson.Address) *Group {
	v, ok := s.groups.Load(gid.String())
	if !ok {
		return nil
	}
	return v.(*Group)
}

func (s *Service) gcGroup() {
	for _, g := range s.getGroupAll() {
		if g.option.GType == model.GTypeKnown {
			if g.keepPeers.Length() > 0 {
				g.keepPeers = pslice.New(1, s.self)
			}
			if g.knownPeers.Length() > 0 {
				g.knownPeers = pslice.New(1, s.self)
			}
			if g.connectedPeers.Length() == 0 {
				s.groups.Delete(g.gid.String())
			}
		}
	}
	return
}

func (s *Service) getGroupOrCreate(gid boson.Address) *Group {
	v, ok := s.groups.Load(gid.String())
	if !ok {
		return s.newGroup(gid, model.ConfigNodeGroup{GType: model.GTypeKnown})
	}
	return v.(*Group)
}

func (s *Service) updatePeerGroupsJoin(peer boson.Address, GIDs []boson.Address) {
	s.peerGroupsMix.Lock()
	defer s.peerGroupsMix.Unlock()
	gs, ok := s.peerGroups[peer.String()]
	if ok {
		for _, g := range gs {
			if !g.gid.MemberOf(GIDs) {
				g.remove(peer, false)
			}
		}
	}
	var now []*Group
	for _, gid := range GIDs {
		g := s.getGroupOrCreate(gid)
		g.add(peer, true)
		now = append(now, g)
	}
	s.peerGroups[peer.String()] = now

}

func (g *Group) update(option model.ConfigNodeGroup) {
	g.mux.Lock()
	defer g.mux.Unlock()
	g.option = option
}

func (g *Group) remove(peer boson.Address, intoKnown bool) {
	g.mux.Lock()
	defer g.mux.Unlock()
	var notify bool
	defer func() {
		if notify {
			g.notifyPeers()
		}
	}()
	if g.connectedPeers.Exists(peer) {
		g.connectedPeers.Remove(peer)
		notify = true
		g.srv.logger.Tracef("group %s remove peer, remove %s from connected", g.gid, peer)
	}
	if g.keepPeers.Exists(peer) {
		g.keepPeers.Remove(peer)
		notify = true
		g.srv.logger.Tracef("group %s remove peer, remove %s from kept", g.gid, peer)
	}
	if g.knownPeers.Exists(peer) {
		if !intoKnown {
			g.knownPeers.Remove(peer)
			g.srv.logger.Tracef("group %s remove peer, remove %s from known", g.gid, peer)
		}
	} else {
		if intoKnown && notify {
			g.knownPeers.Add(peer)
			g.srv.logger.Tracef("group %s remove peer, add %s to known", g.gid, peer)
		}
	}
	return
}

// when keep is true, not direct connect add into g.keepPeers
// else into g.knownPeers
func (g *Group) add(peer boson.Address, keep bool) {
	g.mux.Lock()
	defer g.mux.Unlock()
	var notify bool
	defer func() {
		if notify {
			g.notifyPeers()
		}
	}()
	if !keep {
		if g.connectedPeers.Exists(peer) {
			g.connectedPeers.Remove(peer)
			notify = true
			g.srv.logger.Tracef("group %s add peer, remove %s from connected", g.gid, peer)
		}
		if g.keepPeers.Exists(peer) {
			g.keepPeers.Remove(peer)
			notify = true
			g.srv.logger.Tracef("group %s add peer, remove %s from kept", g.gid, peer)
		}
		if !g.knownPeers.Exists(peer) {
			g.knownPeers.Add(peer)
			g.srv.logger.Tracef("group %s add peer, add %s to known", g.gid, peer)
		}
		return
	}
	// direct connect peer
	if g.srv.route.IsNeighbor(peer) {
		if !g.connectedPeers.Exists(peer) {
			g.connectedPeers.Add(peer)
			notify = true
			g.srv.logger.Tracef("group %s add peer, add %s to connected", g.gid, peer)
		}
		if g.keepPeers.Exists(peer) {
			g.keepPeers.Remove(peer)
			notify = true
			g.srv.logger.Tracef("group %s add peer, remove %s from kept", g.gid, peer)
		}
		if g.knownPeers.Exists(peer) {
			g.knownPeers.Remove(peer)
			g.srv.logger.Tracef("group %s add peer, remove %s from known", g.gid, peer)
		}
		return
	}
	// not direct connect peer
	if g.connectedPeers.Exists(peer) {
		g.connectedPeers.Remove(peer)
		notify = true
		g.srv.logger.Tracef("group %s add peer, remove %s from connected", g.gid, peer)
	}
	if !g.keepPeers.Exists(peer) {
		g.keepPeers.Add(peer)
		notify = true
		g.srv.logger.Tracef("group %s add peer, add %s to kept", g.gid, peer)
	}
	if g.knownPeers.Exists(peer) {
		g.knownPeers.Remove(peer)
		g.srv.logger.Tracef("group %s add peer, remove %s from known", g.gid, peer)
	}
	return
}

func (g *Group) pruneKnown() {
	g.mux.Lock()
	defer g.mux.Unlock()
	k := g.knownPeers.Length() - maxKnownPeers
	if k > 0 {
		peers := g.knownPeers.BinPeers(0)
		for _, peer := range peers {
			g.knownPeers.Remove(peer)
			g.srv.logger.Tracef("group %s pruneKnown, remove %s from known", g.gid, peer)
			k--
			if k <= 0 {
				break
			}
		}
	}
}

func (g *Group) multicast(msg protobuf.Message, skip ...boson.Address) {
	if g.connectedPeers.Length() == 0 && g.keepPeers.Length() == 0 {
		g.srv.discover(g)
	}
	if g.connectedPeers.Length() == 0 && g.keepPeers.Length() == 0 {
		return
	}
	// An isolated node within the group
	send := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if !address.MemberOf(skip) {
			_ = g.srv.sendData(context.Background(), address, streamMulticast, msg)
		}
		return false, false, nil
	}
	_ = g.connectedPeers.EachBin(send)
	_ = g.keepPeers.EachBin(send)
}

func (g *Group) notifyMulticast(msg Message) error {
	if g.multicastSub == false {
		return nil
	}
	return g.srv.subPub.Publish("group", "multicastMsg", g.gid.String(), msg)
}

func (g *Group) getPeers() (out *GroupPeers) {
	out = &GroupPeers{
		Connected: g.srv.getOptimumPeers(g.connectedPeers.BinPeers(0)),
		Keep:      g.srv.getOptimumPeers(g.keepPeers.BinPeers(0)),
	}
	return out
}

func (g *Group) notifyPeers() {
	// prevent other goroutine from continuing
	select {
	case g.groupPeersSending <- struct{}{}:
	default:
		return
	}

	defer func() {
		<-g.groupPeersSending
	}()

	peers := g.getPeers()
	key := "notifyGroupPeers"
	b, _ := json.Marshal(peers)
	has, _ := cache.Contains(cacheCtx, key)
	if has {
		v := cache.MustGet(cacheCtx, key)
		if bytes.Equal(b, v.Bytes()) {
			return
		}
	}
	_ = cache.Set(cacheCtx, key, b, 0)

	var minInterval = time.Millisecond * 500
	ms := time.Since(g.groupPeersLastSend)
	if ms >= minInterval {
		_ = g.srv.subPub.Publish("group", "groupPeers", g.gid.String(), peers)
		g.groupPeersLastSend = time.Now()
	} else {
		<-time.After(minInterval - ms)
		p := g.getPeers()
		_ = g.srv.subPub.Publish("group", "groupPeers", g.gid.String(), p)
		g.groupPeersLastSend = time.Now()
	}
}
