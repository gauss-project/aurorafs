package trafficprotocol

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol/pb"
	"time"
)

const (
	protocolName    = "pseudosettle"
	protocolVersion = "1.0.0"
	streamName      = "traffic" // stream for cheques
	initStreamName  = "init"    // stream for handshake
)

type Interface interface {
	// EmitCheque sends a signed cheque to a peer.
	EmitCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error

	Init(ctx context.Context, peer boson.Address) error
}

type Traffic interface {
	ReceiveCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error

	Handshake(peer boson.Address, beneficiary common.Address, cheque *cheque.SignedCheque) error

	LastReceivedCheque(peer boson.Address) (*cheque.SignedCheque, error)

	UpdatePeerBalance(peer boson.Address) error
}

type Service struct {
	streamer p2p.Streamer
	logging  logging.Logger
	address  common.Address
	traffic  Traffic
}

func New(streamer p2p.Streamer, logging logging.Logger, address common.Address) *Service {
	return &Service{streamer: streamer, logging: logging, address: address}
}

func (s *Service) SetTraffic(traffic Traffic) {
	s.traffic = traffic
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
			{
				Name:    initStreamName,
				Handler: s.initHandler,
			},
		},
	}
}

func (s *Service) initHandler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	var req pb.EmitCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	var c *cheque.SignedCheque
	err = json.Unmarshal(req.SignedCheque, &c)
	if err != nil {
		return err
	}
	s.traffic.Handshake(p.Address, common.BytesToAddress(req.Address), c)

	receiveCheque, _ := s.traffic.LastReceivedCheque(p.Address)

	signedCheque, err := json.Marshal(receiveCheque)
	if err != nil {
		return err
	}

	err = w.WriteMsgWithContext(ctx, &pb.EmitCheque{
		Address:      s.address.Bytes(),
		SignedCheque: signedCheque,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) Init(ctx context.Context, peer boson.Address) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, initStreamName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose() // wait for confirmation
		}
	}()

	receiveCheque, _ := s.traffic.LastReceivedCheque(peer)

	w, r := protobuf.NewWriterAndReader(stream)
	signedCheque, err := json.Marshal(receiveCheque)
	if err != nil {
		return err
	}

	err = w.WriteMsgWithContext(ctx, &pb.EmitCheque{
		Address:      s.address.Bytes(),
		SignedCheque: signedCheque,
	})
	if err != nil {
		return err
	}

	var req pb.EmitCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", peer, err)
	}

	var c *cheque.SignedCheque
	err = json.Unmarshal(req.SignedCheque, &c)
	if err != nil {
		return err
	}
	return s.traffic.Handshake(peer, common.BytesToAddress(req.Address), c)
}

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()
	var req pb.EmitCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	var signedCheque *cheque.SignedCheque
	err = json.Unmarshal(req.SignedCheque, &signedCheque)
	if err != nil {
		return err
	}

	return s.traffic.ReceiveCheque(ctx, p.Address, signedCheque)
}

func (s *Service) EmitCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	s.logging.Tracef("sending cheque message to peer %v (%v)", peer, cheque)

	signedCheque, err := json.Marshal(cheque)
	if err != nil {
		return err
	}

	w := protobuf.NewWriter(stream)
	return w.WriteMsgWithContext(ctx, &pb.EmitCheque{
		SignedCheque: signedCheque,
	})
}
