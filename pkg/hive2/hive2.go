package hive2

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/hive2/pb"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

const (
	protocolName    = "hive2"
	protocolVersion = "1.0.0"
	streamFindNode  = "findNode"

	messageTimeout         = 1 * time.Minute // maximum allowed time for a message to be read or written.
	maxPeersLimit          = 30
	pingTimeout            = time.Second * 5 // time to wait for ping to succeed
	batchValidationTimeout = 5 * time.Minute // prevent lock contention on peer validation

	lookupPoLimit           = 3 // the number of hive2 used to find node for near po count.
	lookupNeighborPeerLimit = 3
	findNodePeerLimit       = 16
)

type Service struct {
	start           bool
	streamer        p2p.StreamerPinger
	addressBook     addressbook.GetPutter
	addPeersHandler func(...boson.Address)
	networkID       uint64
	logger          logging.Logger
	metrics         metrics
	config          Config
	quit            chan struct{}
	wg              sync.WaitGroup
	peersChan       chan resultChan
	sem             *semaphore.Weighted
}

type resultChan struct {
	pb         pb.Peers
	syncResult chan boson.Address
}

type Config struct {
	Kad  *kademlia.Kad
	Base boson.Address
}

func New(streamer p2p.StreamerPinger, addressBook addressbook.GetPutter, networkID uint64, logger logging.Logger) *Service {
	srv := &Service{
		streamer:    streamer,
		logger:      logger,
		addressBook: addressBook,
		networkID:   networkID,
		metrics:     newMetrics(),
		quit:        make(chan struct{}),
		peersChan:   make(chan resultChan),
		sem:         semaphore.NewWeighted(int64(boson.MaxPO)),
	}
	srv.startCheckPeersHandler()
	return srv
}

func (s *Service) SetConfig(config Config) {
	s.config = config
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamFindNode,
				Handler: s.onFindNode,
			},
		},
	}
}

func (s *Service) SetAddPeersHandler(h func(addr ...boson.Address)) {
	s.addPeersHandler = h
}

func (s *Service) Close() error {
	close(s.quit)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.wg.Wait()
	}()

	select {
	case <-stopped:
		return nil
	case <-time.After(time.Second * 5):
		return errors.New("hive2: waited 5 seconds to close active goroutines")
	}
}

func (s *Service) onFindNode(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	s.metrics.OnFindNode.Inc()
	s.logger.Tracef("hive2: onFindNode start... peer=%s", peer.Address.String())
	start := time.Now()
	w, r := protobuf.NewWriterAndReader(stream)
	var req pb.FindNodeReq
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		_ = stream.Reset()
		return fmt.Errorf("hive2: onFindNode handler read message: %w", err)
	}

	defer func() {
		s.logger.Debugf("hive2: onFindNode time consuming %v", time.Since(start).Seconds())
		go stream.FullClose()
	}()

	if req.Limit > maxPeersLimit {
		req.Limit = maxPeersLimit
	}
	resp := &pb.Peers{}

	_ = s.config.Kad.EachPeerRev(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		for _, v := range req.Po {
			if uint8(v) == u {
				p, _ := s.addressBook.Get(address)
				if p != nil {
					resp.Peers = append(resp.Peers, &pb.AuroraAddress{
						Underlay:  p.Underlay.Bytes(),
						Signature: p.Signature,
						Overlay:   p.Overlay.Bytes(),
					})
					if len(resp.Peers) >= int(req.Limit) {
						return true, false, nil
					}
				}
				return false, false, nil
			}
		}
		return false, true, nil
	})

	s.metrics.OnFindNodePeers.Add(float64(len(resp.Peers)))

	err := w.WriteMsgWithContext(ctx, resp)
	if err != nil {
		_ = stream.Reset()
		return fmt.Errorf("hive2: onFindNode handler write message: %w", err)
	}
	return nil
}

func (s *Service) DoFindNode(ctx context.Context, peer boson.Address, pos []int32, limit int32) (res chan boson.Address, total int32, err error) {
	s.metrics.DoFindNode.Inc()
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamFindNode)
	if err != nil {
		s.logger.Errorf("hive2: DoFindNode NewStream, err=%s", err)
		return
	}

	ctx1, cancel := context.WithTimeout(ctx, messageTimeout)
	defer cancel()

	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx1, &pb.FindNodeReq{
		Po:    pos,
		Limit: limit,
	})
	if err != nil {
		_ = stream.Reset()
		err = fmt.Errorf("hive2: DoFindNode write message: %w", err)
		return
	}

	var result pb.Peers
	if err = r.ReadMsgWithContext(ctx1, &result); err != nil {
		_ = stream.Reset()
		err = fmt.Errorf("hive2: DoFindNode read message: %w", err)
		return
	}

	s.metrics.DoFindNodePeers.Add(float64(len(result.Peers)))

	res = make(chan boson.Address)
	select {
	case s.peersChan <- resultChan{
		pb:         result,
		syncResult: res,
	}:
	case <-s.quit:
		return res, 0, errors.New("failed to process peers, shutting down hive2")
	}

	return res, int32(len(result.Peers)), nil
}

func (s *Service) startCheckPeersHandler() {
	ctx, cancel := context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-s.quit
		cancel()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case result := <-s.peersChan:
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					cctx, cancel := context.WithTimeout(ctx, batchValidationTimeout)
					defer cancel()
					s.checkAndAddPeers(cctx, result)
				}()
			}
		}
	}()
}

func (s *Service) checkAndAddPeers(ctx context.Context, result resultChan) {

	var peersToAdd []boson.Address
	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}

	for _, p := range result.pb.Peers {
		err := s.sem.Acquire(ctx, 1)
		if err != nil {
			return
		}

		wg.Add(1)
		go func(newPeer *pb.AuroraAddress) {
			defer func() {
				s.sem.Release(1)
				wg.Done()
			}()

			multiUnderlay, err := ma.NewMultiaddrBytes(newPeer.Underlay)
			if err != nil {
				s.logger.Errorf("hive2: multi address underlay err: %v", err)
				return
			}

			ctx, cancel := context.WithTimeout(ctx, pingTimeout)
			defer cancel()

			// check if the underlay is usable by doing a raw ping using libp2p
			if _, err = s.streamer.Ping(ctx, multiUnderlay); err != nil {
				s.metrics.UnreachablePeers.Inc()
				s.logger.Debugf("hive2: peer %s: underlay %s not reachable", hex.EncodeToString(newPeer.Overlay), multiUnderlay)
				return
			}

			auroraAddress := aurora.Address{
				Overlay:   boson.NewAddress(newPeer.Overlay),
				Underlay:  multiUnderlay,
				Signature: newPeer.Signature,
			}

			err = s.addressBook.Put(auroraAddress.Overlay, auroraAddress)
			if err != nil {
				s.logger.Warningf("hive2: skipping peer in response %s: %v", newPeer.String(), err)
				return
			}

			mtx.Lock()
			peersToAdd = append(peersToAdd, auroraAddress.Overlay)
			mtx.Unlock()
		}(p)
	}

	wg.Wait()

	if s.addPeersHandler != nil && len(peersToAdd) > 0 {
		s.addPeersHandler(peersToAdd...)
	}
	for _, v := range peersToAdd {
		result.syncResult <- v
	}
	close(result.syncResult)
}

func (s *Service) BroadcastPeers(ctx context.Context, addressee boson.Address, peers ...boson.Address) error {

	return nil
}

func (s *Service) IsStart() bool {
	return s.start
}

func (s *Service) Start() {
	go s.discover()
	s.start = true
}

func (s *Service) IsHive2() bool {
	return true
}

// discover is a forever loop that manages the find to new peers
func (s *Service) discover() {
	defer s.wg.Done()
	defer s.logger.Debugf("hive2 discover loop exited")

	worker := func() {
		start := time.Now()
		s.logger.Debugf("hive2 discover start...")
		defer s.logger.Debugf("hive2 discover took %s to finish", time.Since(start))
		stop, jumpNext, _ := s.startFindNode(s.config.Base, 0)
		if stop {
			return
		}
		if jumpNext {
			for i := 0; i < 3; i++ {
				dest := test.RandomAddress()
				stop, _, _ = s.startFindNode(dest, 0)
				if stop {
					return
				}
			}
		}
	}

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
			worker()
		}
	}
}

func (s *Service) startFindNode(target boson.Address, total int32) (stop bool, jumpNext bool, count int32) {
	var ch chan boson.Address
	ch, stop, jumpNext, count = s.lookup(target, total)
	if stop || jumpNext {
		return
	}
	for addr := <-ch; !addr.IsZero(); {
		stop, jumpNext, count = s.startFindNode(addr, count)
		if stop || jumpNext {
			return
		}
	}
	return
}

func (s *Service) lookup(target boson.Address, total int32) (ch chan boson.Address, stop bool, jumpNext bool, count int32) {
	lookupBin := s.config.Kad.NotSaturatedBin()
	if len(lookupBin) == 0 {
		stop = true
		return
	}
	// need discover
	peers, err := s.config.Kad.ClosestPeers(target, lookupNeighborPeerLimit)
	if err != nil {
		s.logger.Warningf("ClosestPeers %s", err)
		return
	}
	for _, dest := range peers {
		pos := lookupDistances(target, dest, lookupPoLimit, lookupBin)
		if len(pos) > 0 {
			var cnt int32
			ch, cnt, _ = s.DoFindNode(context.Background(), dest, pos, findNodePeerLimit-total)
			count = total + cnt
			if count >= findNodePeerLimit {
				jumpNext = true
				return
			}
		}
	}
	jumpNext = true
	return
}

// e.g. lookupRequestLimit=3
// for a target with Proximity(target, dest) = 5 the result is [5, 6, 4].
// skip saturation po
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
