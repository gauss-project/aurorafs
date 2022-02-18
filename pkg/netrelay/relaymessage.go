package netrelay

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
)

const (
	protocolName        = "netrelay"
	protocolVersion     = "2.0.0"
	streamRelayHttpReq  = "httpreq"
	streamRelayHttpResp = "httpresp"
)

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamRelayHttpReq,
				Handler: s.relayHttpReq,
			},
			{
				Name:    streamRelayHttpResp,
				Handler: s.relayHttpResp,
			},
		},
	}
}

func (s *Service) relayHttpReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	return nil
}

func (s *Service) relayHttpResp(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	return nil
}

func (s *Service) SendHttp(ctx context.Context, address boson.Address, msg interface{}) error {
	return nil
}
