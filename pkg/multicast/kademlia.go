package multicast

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/gauss-project/aurorafs/pkg/topology"
	topModel "github.com/gauss-project/aurorafs/pkg/topology/model"
	"github.com/gauss-project/aurorafs/pkg/topology/pslice"
)

const (
	protocolName    = "multicast"
	protocolVersion = "1.2.0"
	streamHandshake = "handshake"
	streamFindGroup = "findGroup"
	streamMulticast = "multicast"
	streamMessage   = "message"
	streamNotify    = "notify"

	handshakeTimeout = time.Second * 10
	keepPingInterval = time.Second * 15

	multicastMsgCache = time.Minute * 1 // According to the message of the whole network arrival time to determine
)

type NotifyStatus int

const (
	NotifyJoinGroup NotifyStatus = iota + 1
	NotifyLeaveGroup
)

type Option struct {
	Dev bool
}

type Service struct {
	o             Option
	nodeMode      aurora.Model
	self          boson.Address
	p2ps          p2p.Service
	stream        p2p.Streamer
	logger        logging.Logger
	kad           topology.Driver
	route         routetab.RouteTab
	groups        sync.Map            // key=gid  value= *Group
	peerGroups    map[string][]*Group // key=peer value= []*Group
	peerGroupsMix sync.Mutex
	msgSeq        uint64
	close         chan struct{}
	sessionStream sync.Map // key= sessionID, value= *WsStream
	subPub        subscribe.SubPub
}

type WsStream struct {
	done       chan struct{} // after w write successful
	sendOption SendOption
	stream     p2p.Stream
	r          protobuf.Reader
	w          protobuf.Writer
}

type PeersSubClient struct {
	notify       *rpc.Notifier
	sub          *rpc.Subscription
	lastPushTime time.Time
}

func NewService(self boson.Address, nodeMode aurora.Model, service p2p.Service, streamer p2p.Streamer, kad topology.Driver, route routetab.RouteTab, logger logging.Logger, subPub subscribe.SubPub, o Option) *Service {
	srv := &Service{
		o:          o,
		nodeMode:   nodeMode,
		self:       self,
		p2ps:       service,
		stream:     streamer,
		logger:     logger,
		kad:        kad,
		route:      route,
		subPub:     subPub,
		close:      make(chan struct{}, 1),
		peerGroups: make(map[string][]*Group),
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
			{
				Name:    streamMessage,
				Handler: s.onMessage,
			},
		},
	}
}

func (s *Service) Start() {
	mNotifier := subscribe.NewNotifierWithMsgChan()
	s.kad.SubscribePeerState(mNotifier)
	go func() {
		ticker := time.NewTicker(keepPingInterval)
		defer func() {
			ticker.Stop()
			close(mNotifier.ErrChan)
		}()

		for {
			select {
			case <-s.close:
				return
			case info := <-mNotifier.MsgChan:
				peer, ok := info.(p2p.PeerInfo)
				if !ok {
					continue
				}
				switch peer.State {
				case p2p.PeerStateConnectOut:
					s.logger.Tracef("event connectOut handshake with group protocol %s", peer.Overlay)
					err := s.Handshake(context.Background(), peer.Overlay)
					if err != nil {
						s.logger.Errorf("multicast handshake %s", err.Error())
					}
				case p2p.PeerStateDisconnect:
					for _, g := range s.getGroupAll() {
						g.remove(peer.Overlay, false)
					}
				}
			case <-ticker.C:
				s.gcGroup()
				t := time.Now()
				s.logger.Debugf("multicast: keep ping start")
				s.HandshakeAllKept(s.getGroupAll(), true)
				s.logger.Debugf("multicast: keep ping done took %s", time.Since(t))
				s.refreshProtectPeers()
				ticker.Reset(keepPingInterval)
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

func (s *Service) refreshProtectPeers() {
	list := s.getAllProtectPeers()
	s.kad.RefreshProtectPeer(list)
}

func (s *Service) Multicast(info *pb.MulticastMsg, skip ...boson.Address) error {
	if len(info.Origin) == 0 {
		info.CreateTime = time.Now().UnixMilli()
		info.Origin = s.self.Bytes()
		info.Id = atomic.AddUint64(&s.msgSeq, 1)
	}
	origin := boson.NewAddress(info.Origin)

	key := fmt.Sprintf("Multicast_%s_%d", origin, info.Id)
	setOK, err := cache.SetIfNotExist(cacheCtx, key, 1, multicastMsgCache)
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

	g := s.getGroup(gid)
	if g != nil {
		g.multicast(info, skip...)
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

func (s *Service) onMulticast(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	r := protobuf.NewReader(stream)
	info := &pb.MulticastMsg{}
	err = r.ReadMsgWithContext(ctx, info)
	if err != nil {
		return err
	}

	origin := boson.NewAddress(info.Origin)

	key := fmt.Sprintf("onMulticast_%s_%d", origin, info.Id)
	setOK, err := cache.SetIfNotExist(cacheCtx, key, 1, multicastMsgCache)
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
	g := s.getGroup(gid)
	if g != nil && g.option.GType == model.GTypeJoin {
		notifyLog = false
		_ = g.notifyMulticast(msg)
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

func (s *Service) observeGroup(gid boson.Address, option model.ConfigNodeGroup) error {
	g := s.getGroup(gid)
	if g == nil {
		g = s.newGroup(gid, option)
	}
	if g.option.GType != model.GTypeJoin {
		g.option = option
		for _, peer := range option.Nodes {
			if peer.Equal(s.self) {
				continue
			}
			g.add(peer, false)
		}
	}
	go s.discover(g)
	s.logger.Infof("add observe group success %s", gid)
	return nil
}

func (s *Service) observeGroupCancel(gid boson.Address) {
	g := s.getGroup(gid)
	if g != nil && g.option.GType == model.GTypeObserve {
		g.update(model.ConfigNodeGroup{
			Name:  g.option.Name,
			GType: model.GTypeKnown,
		})
	}
	s.logger.Infof("remove observe group success %s", gid)
}

// Add yourself to the group, along with other nodes (if any)
func (s *Service) joinGroup(gid boson.Address, option model.ConfigNodeGroup) error {
	g := s.getGroup(gid)
	if g != nil {
		g.option = option
	} else {
		g = s.newGroup(gid, option)
	}
	for _, v := range option.Nodes {
		if v.Equal(s.self) {
			continue
		}
		g.add(v, false)
	}

	go s.HandshakeAll()
	go s.HandshakeAllKept(s.getGroupAll(), false)
	go s.discover(g)

	s.logger.Infof("join group success %s", gid)
	return nil
}

// LeaveGroup For yourself
func (s *Service) leaveGroup(gid boson.Address) error {
	g := s.getGroup(gid)
	if g != nil && g.option.GType == model.GTypeJoin {
		g.update(model.ConfigNodeGroup{
			Name:  g.option.Name,
			GType: model.GTypeKnown,
		})
	}
	go s.HandshakeAll()
	go s.HandshakeAllKept(s.getGroupAll(), false)

	s.logger.Infof("leave group success %s", gid)
	return nil
}

func (s *Service) SubscribeMulticastMsg(n *rpc.Notifier, sub *rpc.Subscription, gid boson.Address) (err error) {
	g := s.getGroup(gid)
	if g == nil || g.option.GType != model.GTypeJoin {
		return errors.New("the group notfound")
	}
	if g.multicastSub == true {
		return errors.New("multicast message subscription already exists")
	}

	notifier := subscribe.NewNotifier(n, sub)
	g.multicastSub = true
	return s.subPub.Subscribe(notifier, "group", "multicastMsg", gid.String())
}

func (s *Service) AddGroup(groups []model.ConfigNodeGroup) error {
	defer s.refreshProtectPeers()
	for _, optionGroup := range groups {
		if optionGroup.Name == "" {
			continue
		}
		var gAddr boson.Address
		addr, err := boson.ParseHexAddress(optionGroup.Name)
		if err != nil {
			gAddr = GenerateGID(optionGroup.Name)
		} else {
			gAddr = addr
		}
		switch optionGroup.GType {
		case model.GTypeJoin:
			err = s.joinGroup(gAddr, optionGroup)
		case model.GTypeObserve:
			err = s.observeGroup(gAddr, optionGroup)
		}
		if err != nil {
			s.logger.Errorf("Groups: Join group failed :%v ", err.Error())
		}
		return err
	}
	return nil
}

func (s *Service) RemoveGroup(gid boson.Address, gType model.GType) error {
	defer s.refreshProtectPeers()
	switch gType {
	case model.GTypeObserve:
		s.observeGroupCancel(gid)
	case model.GTypeJoin:
		return s.leaveGroup(gid)
	default:
		return errors.New("gType not support")
	}
	return nil
}

// Deprecated
func (s *Service) onNotify(ctx context.Context, peer p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.Close()
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
			g := s.getGroupOrCreate(boson.NewAddress(v))
			g.add(peer.Address, true)
			s.logger.Tracef("onNotify join peer %s with gid %s", peer.Address, g.gid)
		}
	case NotifyLeaveGroup: // leave group
		for _, v := range msg.Gids {
			g := s.getGroupOrCreate(boson.NewAddress(v))
			g.remove(peer.Address, false)
			s.logger.Tracef("onNotify remove peer %s with gid %s", peer.Address, g.gid)
		}
	default:
		return errors.New("notify status invalid")
	}
	return nil
}

func (s *Service) sendData(ctx context.Context, address boson.Address, streamName string, msg protobuf.Message) (err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, address, streamName)
	if err != nil {
		s.logger.Error(err)
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

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
		Connected: connected,
		Timestamp: time.Now(),
		Groups:    s.getModelGroupInfo(ss),
	}
}

func (s *Service) getModelGroupInfo(ss map[string]*topModel.PeerInfo) (out []*model.GroupInfo) {
	peersFunc := func(ps *pslice.PSlice) (res []boson.Address) {
		_ = ps.EachBin(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
			res = append(res, address)
			return false, false, err
		})
		return
	}

	list := s.getGroupAll()
	for _, v := range list {
		out = append(out, &model.GroupInfo{
			GroupID:       v.gid,
			Option:        v.option,
			KeepPeers:     s.getOptimumPeers(peersFunc(v.keepPeers)),
			KnowPeers:     s.getOptimumPeers(peersFunc(v.knownPeers)),
			ConnectedInfo: s.getConnectedInfo(peersFunc(v.connectedPeers), ss),
		})
	}
	return out
}

func (s *Service) getConnectedInfo(peers []boson.Address, ss map[string]*topModel.PeerInfo) (out *model.ConnectedInfo) {
	peerInfoFunc := func(list []boson.Address) (infos []*topModel.PeerInfo) {
		for _, v := range list {
			infos = append(infos, ss[v.String()])
		}
		return infos
	}

	return &model.ConnectedInfo{
		Connected:      len(peers),
		ConnectedPeers: peerInfoFunc(peers),
	}
}

func (s *Service) SubscribeLogContent(n *rpc.Notifier, sub *rpc.Subscription) {
	notifier := subscribe.NewNotifier(n, sub)
	_ = s.subPub.Subscribe(notifier, "group", "logContent", "")
	return
}

func (s *Service) notifyLogContent(data LogContent) {
	_ = s.subPub.Publish("group", "logContent", "", data)
}

// GetGroupPeers the peers order by EWMA optimal
func (s *Service) GetGroupPeers(groupName string) (out *GroupPeers, err error) {
	gid, err := boson.ParseHexAddress(groupName)
	if err != nil {
		gid = GenerateGID(groupName)
		err = nil
	}
	g := s.getGroup(gid)
	if g == nil {
		return nil, errors.New("group not found")
	}

	return g.getPeers(), nil
}

func (s *Service) GetOptimumPeer(groupName string) (peer boson.Address, err error) {
	v, err := s.GetGroupPeers(groupName)
	if err != nil {
		return boson.ZeroAddress, err
	}

	if len(v.Connected) > 0 {
		return v.Connected[0], nil
	}
	if len(v.Keep) > 0 {
		return v.Keep[0], nil
	}
	return boson.ZeroAddress, nil
}

func (s *Service) getStream(ctx context.Context, dest boson.Address, streamName string) (stream p2p.Stream, err error) {
	if !s.route.IsNeighbor(dest) {
		stream, err = s.stream.NewConnChainRelayStream(ctx, dest, nil, protocolName, protocolVersion, streamName)
	} else {
		stream, err = s.stream.NewStream(ctx, dest, nil, protocolName, protocolVersion, streamName)
	}
	if err != nil {
		err = fmt.Errorf("p2p stream create failed, %s", err)
	}
	return
}

func (s *Service) Send(ctx context.Context, data []byte, gid, dest boson.Address) (err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, dest, streamMessage)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.Close()
		}
	}()
	req := &pb.GroupMsg{
		Gid:  gid.Bytes(),
		Data: data,
		Type: int32(SendOnly),
	}
	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx, req)
	if err != nil {
		return err
	}

	res := &pb.GroupMsg{}
	err = r.ReadMsgWithContext(ctx, res)
	if err != nil {
		return
	}
	if res.Err != "" {
		err = fmt.Errorf(res.Err)
		return
	}
	return nil
}

func (s *Service) SendReceive(ctx context.Context, data []byte, gid, dest boson.Address) (result []byte, err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, dest, streamMessage)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	req := &pb.GroupMsg{
		Gid:  gid.Bytes(),
		Data: data,
		Type: int32(SendReceive),
	}
	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx, req)
	if err != nil {
		return
	}
	res := &pb.GroupMsg{}
	err = r.ReadMsgWithContext(ctx, res)
	if err != nil {
		return
	}
	if res.Err != "" {
		err = fmt.Errorf(res.Err)
		return
	}
	result = res.Data
	_ = w.WriteMsgWithContext(ctx, nil)
	return
}

func (s *Service) GetSendStream(ctx context.Context, gid, dest boson.Address) (out SendStreamCh, err error) {
	var stream p2p.Stream
	stream, err = s.getStream(ctx, dest, streamMessage)
	if err != nil {
		return
	}
	w, r := protobuf.NewWriterAndReader(stream)
	out.Read = make(chan []byte, 1)
	out.ReadErr = make(chan error, 1)
	out.Write = make(chan []byte, 1)
	out.WriteErr = make(chan error, 1)
	out.Close = make(chan struct{}, 1)
	go func() {
		defer stream.Reset()
		for {
			select {
			case d := <-out.Write:
				err = w.WriteMsgWithContext(ctx, &pb.GroupMsg{Data: d, Gid: gid.Bytes(), Type: int32(SendStream)})
				if err != nil {
					out.WriteErr <- err
					return
				}
			case <-out.Close:
				return
			}
		}
	}()
	go func() {
		defer stream.Reset()
		for {
			res := &pb.GroupMsg{}
			err = r.ReadMsgWithContext(ctx, res)
			if err != nil {
				out.ReadErr <- err
				return
			}
			out.Read <- res.Data
		}
	}()
	return
}

func (s *Service) onMessage(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	r := protobuf.NewReader(stream)
	info := &pb.GroupMsg{}
	err := r.ReadMsgWithContext(ctx, info)
	if err != nil {
		_ = stream.Reset()
		return err
	}

	msg := GroupMessage{
		GID:  boson.NewAddress(info.Gid),
		Data: info.Data,
		From: peer.Address,
	}

	st := &WsStream{
		done:       make(chan struct{}, 1),
		sendOption: SendOption(info.Type),
		stream:     stream,
		w:          protobuf.NewWriter(stream),
		r:          r,
	}

	var (
		notifyErr error
	)
	g := s.getGroup(msg.GID)
	if g != nil && g.option.GType == model.GTypeJoin {
		if g.groupMsgSub == false {
			notifyErr = fmt.Errorf("target not subscribe the group message")
		}
		_ = s.notifyMessage(g, msg, st)
	} else {
		notifyErr = fmt.Errorf("target not in the group")
	}
	if notifyErr != nil {
		err = protobuf.NewWriter(stream).WriteMsgWithContext(ctx, &pb.GroupMsg{
			Gid:  info.Gid,
			Type: info.Type,
			Err:  notifyErr.Error(),
		})
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}
	return nil
}

func (s *Service) SubscribeGroupMessage(n *rpc.Notifier, sub *rpc.Subscription, gid boson.Address) (err error) {
	notifier := subscribe.NewNotifier(n, sub)
	g := s.getGroup(gid)
	if g != nil && g.option.GType == model.GTypeJoin {
		g.groupMsgSub = true
		_ = s.subPub.Subscribe(notifier, "group", "groupMessage", gid.String())
		return nil
	}
	return errors.New("the joined group notfound")
}

func (s *Service) notifyMessage(g *Group, msg GroupMessage, st *WsStream) (e error) {
	if g.groupMsgSub == false {
		return nil
	}
	defer func() {
		err := recover()
		if err != nil {
			e = fmt.Errorf("group %s , notify msg %s", g.gid, err)
			s.logger.Error(e)
			g.groupMsgSub = false
		}
	}()

	if st.sendOption != SendOnly {
		msg.SessionID = rpc.NewID()
	}

	_ = s.subPub.Publish("group", "groupMessage", g.gid.String(), msg)

	s.logger.Debugf("group: sessionID %s %s from %s", msg.SessionID, st.sendOption, msg.From)
	switch st.sendOption {
	case SendOnly:
		err := st.w.WriteMsg(&pb.GroupMsg{})
		if err != nil {
			s.logger.Tracef("group: sessionID %s reply err %v", msg.SessionID, err)
			_ = st.stream.Reset()
		} else {
			s.logger.Tracef("group: sessionID %s reply success", msg.SessionID)
			go st.stream.FullClose()
		}
	case SendReceive:
		go func() {
			s.sessionStream.Store(msg.SessionID, st)
			defer s.sessionStream.Delete(msg.SessionID)
			timeout := time.Second * 30
			select {
			case <-time.After(timeout):
				s.logger.Debugf("group: sessionID %s timeout %s when wait receive reply from websocket", msg.SessionID, timeout)
				_ = st.stream.Reset()
			case <-st.done:
				_ = st.stream.FullClose()
			}
		}()
		go func() {
			defer close(st.done)
			for {
				var nothing protobuf.Message
				err := st.r.ReadMsg(nothing)
				s.logger.Tracef("group: sessionID %s close from the sender %v", msg.SessionID, err)
				return
			}
		}()
	case SendStream:
	}
	return nil
}

func (s *Service) replyGroupMessage(sessionID string, data []byte) (err error) {
	v, ok := s.sessionStream.Load(rpc.ID(sessionID))
	if !ok {
		s.logger.Tracef("group: sessionID %s reply err invalid or has expired", sessionID)
		return fmt.Errorf("sessionID %s is invalid or has expired", sessionID)
	}
	st := v.(*WsStream)
	defer func() {
		if err != nil {
			s.logger.Errorf("group: sessionID %s reply err %v", sessionID, err)
			_ = st.stream.Reset()
		} else {
			switch st.sendOption {
			case SendReceive:
				s.logger.Tracef("group: sessionID %s reply success", sessionID)
			}
		}
	}()
	return st.w.WriteMsg(&pb.GroupMsg{
		Data: data,
	})
}

func (s *Service) subscribeGroupPeers(n *rpc.Notifier, sub *rpc.Subscription, gid boson.Address) (err error) {
	notifier := subscribe.NewNotifier(n, sub)
	g := s.getGroup(gid)
	if g == nil {
		return errors.New("the group notfound")
	}
	_ = s.subPub.Subscribe(notifier, "group", "groupPeers", gid.String())
	go func() {
		_ = n.Notify(sub.ID, g.getPeers())
	}()
	return nil
}

func (s *Service) getOptimumPeers(list []boson.Address) (now []boson.Address) {
	return s.kad.GetPeersWithLatencyEWMA(list)
}
