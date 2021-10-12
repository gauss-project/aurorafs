package hive2

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
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

	target := boson.NewAddress(req.Target)
	_ = s.config.Kad.EachPeer(func(address boson.Address, u uint8) (stop, jumpToNext bool, err error) {
		if address.Equal(peer.Address) {
			return false, false, nil
		}
		po := boson.Proximity(target.Bytes(), address.Bytes())
		if inArray(po, req.Pos) {
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
		}
		return false, false, nil
	})

	s.metrics.OnFindNodePeers.Add(float64(len(resp.Peers)))

	err := w.WriteMsgWithContext(ctx, resp)
	if err != nil {
		_ = stream.Reset()
		return fmt.Errorf("hive2: onFindNode handler write message: %w", err)
	}
	return nil
}

func (s *Service) DoFindNode(ctx context.Context, target, peer boson.Address, pos []int32, limit int32) (res chan boson.Address, err error) {
	s.metrics.DoFindNode.Inc()
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamFindNode)
	if err != nil {
		s.logger.Debugf("hive2: DoFindNode NewStream %s, err=%s", peer.String(), err)
		return
	}

	ctx1, cancel := context.WithTimeout(ctx, messageTimeout)
	defer cancel()

	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx1, &pb.FindNodeReq{
		Target: target.Bytes(),
		Pos:    pos,
		Limit:  limit,
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

	res = make(chan boson.Address, 1)
	select {
	case s.peersChan <- resultChan{
		pb:         result,
		syncResult: res,
	}:
	case <-s.quit:
		return res, errors.New("failed to process peers, shutting down hive2")
	}

	return res, nil
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

			if s.addPeersHandler != nil {
				s.addPeersHandler(auroraAddress.Overlay)
			}
			<-time.After(time.Millisecond * 200)
			result.syncResult <- auroraAddress.Overlay

		}(p)
	}

	wg.Wait()

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
