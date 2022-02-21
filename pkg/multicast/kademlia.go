package multicast

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/topology"
	topModel "github.com/gauss-project/aurorafs/pkg/topology/model"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
	"github.com/gogf/gf/os/gcache"
	"github.com/gogf/gf/util/gconv"
)

const (
	protocolName    = "multicast"
	protocolVersion = "1.0.0"
	streamHandshake = "handshake"
	streamFindGroup = "findGroup"
	streamMulticast = "multicast"
	streamNotify    = "notify"

	handshakeTimeout = time.Second * 15
	keepPingInterval = time.Second * 30

	multicastMsgCache = time.Minute * 1 // According to the message of the whole network arrival time to determine
)

type NotifyStatus int

const (
	NotifyJoinGroup NotifyStatus = iota + 1
	NotifyLeaveGroup
)

type Service struct {
	o              Option
	self           boson.Address
	p2ps           p2p.Service
	stream         p2p.Streamer
	logger         logging.Logger
	kad            topology.Driver
	route          routetab.RouteTab
	connectedPeers sync.Map // key=gid, slice is peer, all is neighbor
	groups         sync.Map
	msgSeq         uint64
	close          chan struct{}

	logSig    []chan LogContent
	logSigMtx sync.Mutex
}

type Group struct {
	gid            boson.Address
	connectedPeers *pslice.PSlice
	keepPeers      *pslice.PSlice // Need to maintain the connection with ping
	knownPeers     *pslice.PSlice
	srv            *Service
	option         model.GroupOption
	msgChan        chan Message
}

func (s *Service) newGroup(gid boson.Address, ch chan Message, o model.GroupOption) *Group {
	if o.KeepConnectedPeers < 0 {
		o.KeepConnectedPeers = 0
	}
	if o.KeepPingPeers < 0 {
		o.KeepPingPeers = 0
	}
	g := &Group{
		gid:        gid,
		keepPeers:  pslice.New(1, s.self),
		knownPeers: pslice.New(1, s.self),
		srv:        s,
		option:     o,
		msgChan:    ch,
	}
	conn, ok := s.connectedPeers.Load(gid.String())
	if ok {
		g.connectedPeers = conn.(*pslice.PSlice)
	} else {
		g.connectedPeers = pslice.New(1, s.self)
	}
	return g
}

type Option struct {
	Dev bool
}

func NewService(self boson.Address, service p2p.Service, streamer p2p.Streamer, kad topology.Driver, route routetab.RouteTab, logger logging.Logger, o Option) *Service {
	srv := &Service{
		o:      o,
		self:   self,
		p2ps:   service,
		stream: streamer,
		logger: logger,
		kad:    kad,
		route:  route,
		close:  make(chan struct{}, 1),
	}
	return srv
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamHandshake,
				Handler: s.HandshakeIncoming,
			},
			{
				Name:    streamFindGroup,
				Handler: s.onFindGroup,
			},
			{
				Name:    streamMulticast,
				Handler: s.onMulticast,
			},
			{
				Name:    streamNotify,
				Handler: s.onNotify,
			},
		},
	}
}

func (s *Service) Start() {
	chanDisconnect, cancelDisconnect := s.kad.SubscribePeerState(p2p.PeerStateDisconnect)
	chanConnectOut, cancelConnectOut := s.kad.SubscribePeerState(p2p.PeerStateConnectOut)
	go func() {
		ticker := time.NewTicker(keepPingInterval)
		defer func() {
			ticker.Stop()
			cancelDisconnect()
			cancelConnectOut()
		}()

		for {
			select {
			case <-s.close:
				return
			case peer := <-chanDisconnect:
				s.leaveConnectedAll(peer.Address)
			case peer := <-chanConnectOut:
				s.logger.Tracef("event connectOut handshake with group protocol %s", peer.Address)
				err := s.Handshake(context.Background(), peer.Address)
				if err != nil {
					s.logger.Errorf("multicast handshake %s", err.Error())
				}
			case <-ticker.C:
				s.groups.Range(func(_, value interface{}) bool {
					v := value.(*Group)
					_ = v.keepPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
						err = s.Handshake(context.Background(), address)
						if err != nil {
							s.logger.Tracef("keep ping %s %s", address, err)
							v.keepPeers.Remove(address)

							if v.knownPeers.Length() >= maxKnownPeers {
								p := RandomPeer(v.knownPeers.BinPeers(0))
								v.knownPeers.Remove(p)
							}

							v.knownPeers.Add(address)
						}
						return false, false, nil
					})
					return true
				})
			}
		}
	}()

	// discover
	if !s.o.Dev {
		s.StartDiscover()
	}
}

func (s *Service) Close() error {
	close(s.close)
	return nil
}

func (s *Service) connectedAddToGroup(gid boson.Address, peers ...boson.Address) {
	var conn *pslice.PSlice
	v, ok := s.connectedPeers.Load(gid.String())
	if ok {
		conn = v.(*pslice.PSlice)
	} else {
		g, has := s.groups.Load(gid.String())
		if has {
			conn = g.(*Group).connectedPeers
		} else {
			conn = pslice.New(1, s.self)
		}
		s.connectedPeers.Store(gid.String(), conn)
	}

	for _, p := range peers {
		conn.Add(p)
		s.groups.Range(func(_, value interface{}) bool {
			gr := value.(*Group)
			gr.keepPeers.Remove(p)
			gr.knownPeers.Remove(p)
			return true
		})
	}
}

func (s *Service) keepAddToGroup(gid boson.Address, peers ...boson.Address) {
	v, ok := s.groups.Load(gid.String())
	if ok {
		gr := v.(*Group)
		for _, addr := range peers {
			if !s.route.IsNeighbor(addr) {
				gr.keepPeers.Add(addr)
				gr.knownPeers.Remove(addr)
			}
		}
	}
}

func (s *Service) connectedRemoveFromGroup(gid boson.Address, peers ...boson.Address) {
	v, ok := s.connectedPeers.Load(gid.String())
	if ok {
		conn := v.(*pslice.PSlice)
		for _, addr := range peers {
			conn.Remove(addr)
		}
		if conn.Length() == 0 {
			s.connectedPeers.Delete(gid.String())
		}
	}
}

func (s *Service) leaveConnectedAll(peers ...boson.Address) {
	s.connectedPeers.Range(func(key, value interface{}) bool {
		conn := value.(*pslice.PSlice)
		for _, v := range peers {
			conn.Remove(v)
		}
		if conn.Length() == 0 {
			s.connectedPeers.Delete(key)
		}
		return true
	})
}

func (s *Service) getGIDsByte() [][]byte {
	GIDs := make([][]byte, 0)
	s.groups.Range(func(key, value interface{}) bool {
		gid := boson.MustParseHexAddress(gconv.String(key))
		g := value.(*Group)
		if !g.option.Observe {
			GIDs = append(GIDs, gid.Bytes())
		}
		return true
	})
	return GIDs
}

func (s *Service) Multicast(info *pb.MulticastMsg, skip ...boson.Address) error {
	if len(info.Origin) == 0 {
		info.CreateTime = time.Now().UnixMilli()
		info.Origin = s.self.Bytes()
		info.Id = atomic.AddUint64(&s.msgSeq, 1)
	}
	origin := boson.NewAddress(info.Origin)

	key := fmt.Sprintf("Multicast_%s_%d", origin, info.Id)
	setOK, err := gcache.SetIfNotExist(key, 1, multicastMsgCache)
	if err != nil {
		return err
	}
	if !setOK {
		return nil
	}

	gid := boson.NewAddress(info.Gid)

	s.logger.Tracef("multicast deliver: %s data=%v", key, info.Data)
	s.notifyLogContent(LogContent{
		Event: "multicast_deliver",
		Time:  time.Now().UnixMilli(),
		Data: Message{
			ID:         info.Id,
			CreateTime: info.CreateTime,
			GID:        gid,
			Origin:     origin,
			Data:       info.Data,
		},
	})

	g, ok := s.groups.Load(gid.String())
	if ok {
		v := g.(*Group)
		if v.connectedPeers.Length() == 0 && v.keepPeers.Length() == 0 {
			s.discover()
		}
		if v.connectedPeers.Length() == 0 && v.keepPeers.Length() == 0 {
			return nil
		}
		// An isolated node within the group
		send := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if !address.MemberOf(skip) {
				_ = s.sendData(context.Background(), address, streamMulticast, info)
			}
			return false, false, nil
		}
		_ = v.connectedPeers.EachBin(send)
		_ = v.keepPeers.EachBin(send)
		return nil
	}

	nodes := s.getForwardNodes(gid, skip...)
	s.logger.Tracef("multicast got forward %d nodes", len(nodes))
	for _, v := range nodes {
		s.logger.Tracef("multicast forward to %s", v)
		_ = s.sendData(context.Background(), v, streamMulticast, info)
	}
	return nil
}

func (s *Service) onMulticast(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	info := &pb.MulticastMsg{}
	err := r.ReadMsgWithContext(ctx, info)
	if err != nil {
		return err
	}

	origin := boson.NewAddress(info.Origin)

	key := fmt.Sprintf("onMulticast_%s_%d", origin, info.Id)
	setOK, err := gcache.SetIfNotExist(key, 1, multicastMsgCache)
	if err != nil {
		return err
	}
	if !setOK {
		return nil
	}
	if origin.Equal(s.self) {
		return nil
	}

	gid := boson.NewAddress(info.Gid)
	msg := Message{
		ID:         info.Id,
		CreateTime: info.CreateTime,
		GID:        gid,
		Origin:     origin,
		Data:       info.Data,
		From:       peer.Address,
	}

	s.logger.Tracef("multicast receive: %s data=%v", key, info.Data)
	s.logger.Tracef("multicast receive from %s", peer.Address)

	notifyLog := true
	g, ok := s.groups.Load(gid.String())
	if ok && !g.(*Group).option.Observe {
		notifyLog = false
		_ = s.notifyMsg(gid, msg)
		s.logger.Tracef("%s-multicast receive %s from %s", gid, key, peer.Address)
	}
	if notifyLog {
		s.notifyLogContent(LogContent{
			Event: "multicast_receive",
			Time:  time.Now().UnixMilli(),
			Data:  msg,
		})
	}
	return s.Multicast(info, peer.Address)
}

func (s *Service) notifyMsg(gid boson.Address, msg Message) (e error) {
	g, ok := s.groups.Load(gid.String())
	if ok {
		v := g.(*Group)
		if v.msgChan == nil {
			return nil
		}
		defer func() {
			err := recover()
			if err != nil {
				e = fmt.Errorf("group %s , notify msg %s", gid, err)
				s.logger.Error(e)
				v.msgChan = nil
			}
		}()

		select {
		case v.msgChan <- msg:
		default:
		}
	}
	return nil
}

func (s *Service) ObserveGroup(gid boson.Address, option model.GroupOption, peers ...boson.Address) error {
	var g *Group
	v, ok := s.groups.Load(gid.String())
	if ok {
		g = v.(*Group)
	} else {
		option.Observe = true
		g = s.newGroup(gid, nil, option)
		s.groups.Store(gid.String(), g)
	}
	for _, addr := range peers {
		if addr.Equal(s.self) {
			continue
		}
		if s.route.IsNeighbor(addr) {
			g.connectedPeers.Add(addr)
		} else {
			g.keepPeers.Add(addr)
		}
	}
	return nil
}

func (s *Service) ObserveGroupCancel(gid boson.Address) error {
	g, ok := s.groups.Load(gid.String())
	if !ok {
		return errors.New("group not found")
	}
	v := g.(*Group)
	if v.option.Observe {
		s.groups.Delete(gid.String())
	}
	return nil
}

// JoinGroup Add yourself to the group, along with other nodes (if any)
func (s *Service) JoinGroup(ctx context.Context, gid boson.Address, ch chan Message, option model.GroupOption, peers ...boson.Address) error {
	var g *Group
	value, ok := s.groups.Load(gid.String())
	if ok {
		g = value.(*Group)
		if !g.option.Observe {
			return errors.New("it's already in the group")
		}
	} else {
		g = s.newGroup(gid, ch, option)
		s.groups.Store(gid.String(), g)
	}
	if g.option.Observe {
		// observe group join group
		g.option.Observe = false
	}
	for _, v := range peers {
		if v.Equal(s.self) {
			continue
		}
		if s.route.IsNeighbor(v) {
			g.connectedPeers.Add(v)
		} else {
			g.keepPeers.Add(v)
		}
	}

	go s.notify(&pb.Notify{
		Status: int32(NotifyJoinGroup),
		Gids:   [][]byte{gid.Bytes()},
	})

	if ch != nil {
		go func() {
			<-ctx.Done()
			g.msgChan = nil
		}()
	}

	s.logger.Infof("join group success %s", gid)
	return nil
}

func (s *Service) SubscribeMulticastMsg(gid boson.Address) (c <-chan Message, unsubscribe func(), err error) {
	unsubscribe = func() {}
	channel := make(chan Message, 1)
	var g *Group
	value, ok := s.groups.Load(gid.String())
	if ok {
		g = value.(*Group)
		if !g.option.Observe && g.msgChan == nil {
			g.msgChan = channel
			return channel, func() { g.msgChan = nil }, nil
		}
		if g.msgChan != nil {
			return nil, unsubscribe, errors.New("multicast message subscription already exists")
		}
	}
	return nil, unsubscribe, errors.New("the group notfound")
}

func (s *Service) GetMulticastNode(groupName string) (peer boson.Address, err error) {
	peer = boson.ZeroAddress
	gid := GenerateGID(groupName)
	v, ok := s.groups.Load(gid.String())
	if !ok {
		return peer, errors.New("group not found")
	}

	group := v.(*Group)
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	nodeCount := group.connectedPeers.Length()

	i := 0
	if nodeCount > 0 {
		randKey := rd.Intn(nodeCount)
		group.connectedPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if i == randKey {
				peer = address
				return true, false, nil
			} else {
				return false, true, nil
			}
		})
		if !peer.Equal(boson.ZeroAddress) {
			return peer, nil
		}
	}

	nodeCount = group.keepPeers.Length()
	if nodeCount > 0 {
		randKey := rd.Intn(nodeCount)
		group.keepPeers.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			if i == randKey {
				peer = address
				return true, false, nil
			} else {
				return false, true, nil
			}
		})
		if !peer.Equal(boson.ZeroAddress) {
			return peer, nil
		}
	}
	return boson.ZeroAddress, nil
}

func (s *Service) AddGroup(ctx context.Context, groups []aurora.ConfigNodeGroup) error {
	var peers []boson.Address
	var gAddr boson.Address
	for _, optionGroup := range groups {
		if optionGroup.Name == "" {
			continue
		}

		for _, v := range optionGroup.Nodes {
			addr, err := boson.ParseHexAddress(v)
			if err != nil {
				return fmt.Errorf("add group %w", err)
			}
			peers = append(peers, addr)
		}

		addr, err := boson.ParseHexAddress(optionGroup.Name)
		if err != nil {
			gAddr = GenerateGID(optionGroup.Name)
		} else {
			gAddr = addr
		}
		option := model.GroupOption{
			KeepConnectedPeers: optionGroup.KeepConnectedPeers,
			KeepPingPeers:      optionGroup.KeepPingPeers,
		}
		switch optionGroup.GType {
		case 0:
			err = s.JoinGroup(ctx, gAddr, nil, option, peers...)
		case 1:
			option.Observe = true
			err = s.ObserveGroup(gAddr, option, peers...)
		}
		if err != nil {
			s.logger.Errorf("Groups: Join group failed :%v ", err.Error())
		}
		return err
	}
	return nil
}

// LeaveGroup For yourself
func (s *Service) LeaveGroup(gid boson.Address) error {
	value, ok := s.groups.Load(gid.String())
	if !ok {
		return errors.New("group not found")
	}
	g := value.(*Group)
	if g.connectedPeers.Length() == 0 {
		s.connectedPeers.Delete(gid.String())
	}
	g.msgChan = nil

	copyGroups := make([]*Group, 0)
	s.groups.Range(func(_, value interface{}) bool {
		v := value.(*Group)
		copyGroups = append(copyGroups, v)
		return true
	})
	s.groups.Delete(gid.String())

	go s.notify(&pb.Notify{
		Status: int32(NotifyLeaveGroup),
		Gids:   [][]byte{gid.Bytes()},
	}, copyGroups...)

	s.logger.Infof("leave group success %s", gid)
	return nil
}

func (s *Service) notify(msg *pb.Notify, groups ...*Group) {
	send := func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		_ = s.sendData(context.Background(), address, streamNotify, msg)
		return false, false, nil
	}
	_ = s.kad.EachPeerRev(send)

	if len(groups) == 0 {
		s.groups.Range(func(_, value interface{}) bool {
			v := value.(*Group)
			_ = v.keepPeers.EachBin(send)
			return true
		})
	} else {
		for _, v := range groups {
			_ = v.keepPeers.EachBin(send)
		}
	}
}

func (s *Service) onNotify(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	r := protobuf.NewReader(stream)
	var msg pb.Notify
	err = r.ReadMsgWithContext(ctx, &msg)
	if err != nil {
		return err
	}

	switch NotifyStatus(msg.Status) {
	case NotifyJoinGroup: // join  group
		for _, v := range msg.Gids {
			gid := boson.NewAddress(v)
			if s.route.IsNeighbor(peer.Address) {
				s.connectedAddToGroup(gid, peer.Address)
				s.logger.Tracef("onNotify connected %s with gid %s", peer.Address, gid)
			} else {
				s.keepAddToGroup(gid, peer.Address)
				s.logger.Tracef("onNotify keep %s with gid %s", peer.Address, gid)
			}
		}
	case NotifyLeaveGroup: // leave group
		for _, v := range msg.Gids {
			gid := boson.NewAddress(v)
			s.connectedRemoveFromGroup(gid, peer.Address)
			s.logger.Tracef("onNotify remove connected %s with gid %s", peer.Address, gid)
			value, ok := s.groups.Load(gid.String())
			if ok {
				g := value.(*Group)
				g.keepPeers.Remove(peer.Address)
				g.knownPeers.Remove(peer.Address)
			}
		}
	default:
		return errors.New("notify status invalid")
	}
	return nil
}

func (s *Service) sendData(ctx context.Context, address boson.Address, streamName string, msg protobuf.Message) (err error) {
	var stream p2p.Stream
	if !s.route.IsNeighbor(address) {
		stream, err = s.stream.NewRelayStream(ctx, address, nil, protocolName, protocolVersion, streamName, false)
	} else {
		stream, err = s.stream.NewStream(ctx, address, nil, protocolName, protocolVersion, streamName)
	}
	if err != nil {
		s.logger.Error(err)
		return err
	}
	w := protobuf.NewWriter(stream)
	err = w.WriteMsgWithContext(ctx, msg)
	if err != nil {
		s.logger.Errorf("%s/%s/%s send data to %s %s", protocolName, protocolVersion, streamName, address, err.Error())
		return err
	}
	return nil
}

func (s *Service) Snapshot() *model.KadParams {
	connected, ss := s.kad.SnapshotConnected()

	return &model.KadParams{
		Connected:     connected,
		Timestamp:     time.Now(),
		Groups:        s.getModelGroupInfo(),
		ConnectedInfo: s.getConnectedInfo(ss),
	}
}

func (s *Service) getModelGroupInfo() (out []*model.GroupInfo) {
	peersFunc := func(ps *pslice.PSlice) (res []boson.Address) {
		_ = ps.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			res = append(res, address)
			return false, false, err
		})
		return
	}

	s.groups.Range(func(_, value interface{}) bool {
		v := value.(*Group)
		out = append(out, &model.GroupInfo{
			GroupID:   v.gid,
			Option:    v.option,
			KeepPeers: peersFunc(v.keepPeers),
			KnowPeers: peersFunc(v.knownPeers),
		})
		return true
	})
	return out
}

func (s *Service) getConnectedInfo(ss map[string]*topModel.PeerInfo) (out []*model.ConnectedInfo) {
	peerInfoFunc := func(list []boson.Address) (infos []*topModel.PeerInfo) {
		for _, v := range list {
			infos = append(infos, ss[v.String()])
		}
		return infos
	}
	s.connectedPeers.Range(func(key, value interface{}) bool {
		gid := boson.MustParseHexAddress(gconv.String(key))
		v := value.(*pslice.PSlice)
		out = append(out, &model.ConnectedInfo{
			GroupID:        gid,
			Connected:      v.Length(),
			ConnectedPeers: peerInfoFunc(v.BinPeers(0)),
		})
		return true
	})
	return out
}

func (s *Service) SubscribeLogContent() (c <-chan LogContent, unsubscribe func()) {
	channel := make(chan LogContent, 1)
	var closeOnce sync.Once

	s.logSigMtx.Lock()
	defer s.logSigMtx.Unlock()

	s.logSig = append(s.logSig, channel)

	unsubscribe = func() {
		s.logSigMtx.Lock()
		defer s.logSigMtx.Unlock()

		for i, c := range s.logSig {
			if c == channel {
				s.logSig = append(s.logSig[:i], s.logSig[i+1:]...)
				break
			}
		}

		closeOnce.Do(func() { close(channel) })
	}

	return channel, unsubscribe
}

func (s *Service) notifyLogContent(data LogContent) {
	s.logSigMtx.Lock()
	defer s.logSigMtx.Unlock()

	for _, c := range s.logSig {
		// Every logSig channel has a buffer capacity of 1,
		// so every receiver will get the signal even if the
		// select statement has the default case to avoid blocking.
		select {
		case c <- data:
		default:
		}
	}
}
