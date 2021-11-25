package trafficprotocol

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
)

const (
	protocolName    = "traffic"
	protocolVersion = "1.0.0"
	streamName      = "traffic" // stream for cheques
	initStreamName  = "init"    // stream for handshake
)

type Traffic interface {
	ReceiveCheque(ctx context.Context, peer boson.Address, cheque *cheque.SignedCheque) error

	Handshake(peer boson.Address, beneficiary common.Address, cheque *cheque.SignedCheque) error

	LastReceivedCheque(chainAddress common.Address) (*cheque.SignedCheque, error)
}

type Service struct {
	streamer p2p.Streamer
	logging  logging.Logger
	traffic  Traffic
}

func New(streamer p2p.Streamer, logging logging.Logger, traffic Traffic) *Service {
	return &Service{streamer: streamer, logging: logging, traffic: traffic}
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
	return nil
}

func (s *Service) init(ctx context.Context, p p2p.Peer) error {
	return nil
}

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	return nil
}
