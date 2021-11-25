package trafficprotocol

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol/pb"
	"math/big"
	"time"
)

const (
	protocolName    = "traffic"
	protocolVersion = "1.0.0"
	streamName      = "traffic" // stream for cheques
	initStreamName  = "init"    // stream for handshake
)

type Interface interface {
	// EmitCheque sends a signed cheque to a peer.
	EmitCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error
}

type Traffic interface {
	ReceiveCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error

	Handshake(peer boson.Address, beneficiary common.Address, cheque *cheque.SignedCheque) error

	LastReceivedCheque(peer boson.Address) (*cheque.SignedCheque, error)
}

type Service struct {
	streamer p2p.Streamer
	logging  logging.Logger
	traffic  Traffic
}

func New(streamer p2p.Streamer, logging logging.Logger) *Service {
	return &Service{streamer: streamer, logging: logging}
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
			_ = stream.FullClose()
		}
	}()
	var req pb.SignedCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	receiveCheque, err := s.traffic.LastReceivedCheque(p.Address)
	if err != nil {
		return fmt.Errorf("failed to get the last cheque")
	}

	sendSignedCheque := &pb.SignedCheque{
		Cheque: &pb.Cheque{
			Recipient:        receiveCheque.Recipient.Bytes(),
			Beneficiary:      receiveCheque.Beneficiary.Bytes(),
			CumulativePayout: receiveCheque.CumulativePayout.Uint64(),
		},
		Signature: receiveCheque.Signature,
	}

	err = w.WriteMsgWithContext(ctx, sendSignedCheque)
	if err != nil {
		return err
	}

	beneficiary := common.BytesToAddress(req.Cheque.Beneficiary)
	resCheque := cheque.Cheque{
		Beneficiary:      common.BytesToAddress(req.Cheque.Beneficiary),
		Recipient:        common.BytesToAddress(req.Cheque.Recipient),
		CumulativePayout: new(big.Int).SetUint64(req.Cheque.CumulativePayout),
	}
	signedCheque := &cheque.SignedCheque{
		Cheque:    resCheque,
		Signature: req.Signature,
	}
	return s.traffic.Handshake(p.Address, beneficiary, signedCheque)

}

func (s *Service) init(ctx context.Context, p p2p.Peer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, p.Address, nil, protocolName, protocolVersion, initStreamName)
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

	receiveCheque, err := s.traffic.LastReceivedCheque(p.Address)
	if err != nil {
		return fmt.Errorf("failed to get the last cheque")
	}

	w, r := protobuf.NewWriterAndReader(stream)
	sendSignedCheque := &pb.SignedCheque{
		Cheque: &pb.Cheque{
			Recipient:        receiveCheque.Recipient.Bytes(),
			Beneficiary:      receiveCheque.Beneficiary.Bytes(),
			CumulativePayout: receiveCheque.CumulativePayout.Uint64(),
		},
		Signature: receiveCheque.Signature,
	}

	err = w.WriteMsgWithContext(ctx, sendSignedCheque)
	if err != nil {
		return err
	}

	var req pb.SignedCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	beneficiary := common.BytesToAddress(req.Cheque.Beneficiary)
	resCheque := cheque.Cheque{
		Beneficiary:      common.BytesToAddress(req.Cheque.Beneficiary),
		Recipient:        common.BytesToAddress(req.Cheque.Recipient),
		CumulativePayout: new(big.Int).SetUint64(req.Cheque.CumulativePayout),
	}
	signedCheque := &cheque.SignedCheque{
		Cheque:    resCheque,
		Signature: req.Signature,
	}
	return s.traffic.Handshake(p.Address, beneficiary, signedCheque)
}

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	var req pb.SignedCheque
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	var signedCheque *cheque.SignedCheque
	signedCheque.Cheque.Beneficiary = common.BytesToAddress(req.Cheque.Beneficiary)
	signedCheque.Cheque.Recipient = common.BytesToAddress(req.Cheque.Recipient)
	signedCheque.Cheque.CumulativePayout = new(big.Int).SetUint64(req.Cheque.CumulativePayout)
	signedCheque.Signature = req.Signature

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

	emitCheque := &pb.SignedCheque{
		Cheque: &pb.Cheque{
			Beneficiary:      cheque.Cheque.Beneficiary.Bytes(),
			Recipient:        cheque.Cheque.Recipient.Bytes(),
			CumulativePayout: cheque.Cheque.CumulativePayout.Uint64(),
		},
		Signature: cheque.Signature,
	}

	w := protobuf.NewWriter(stream)
	return w.WriteMsgWithContext(ctx, emitCheque)
}
