package libp2p

import (
	"context"

	handshake "github.com/gauss-project/aurorafs/pkg/p2p/libp2p/internal/handshake"
	libp2pm "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	libp2ppeer "github.com/libp2p/go-libp2p-core/peer"
)

func (s *Service) HandshakeService() *handshake.Service {
	return s.handshakeService
}

func (s *Service) NewStreamForPeerID(peerID libp2ppeer.ID, protocolName, protocolVersion, streamName string) (network.Stream, error) {
	return s.newStreamForPeerID(context.Background(), peerID, protocolName, protocolVersion, streamName)
}

type StaticAddressResolver = staticAddressResolver

var NewStaticAddressResolver = newStaticAddressResolver

func WithHostFactory(factory func(context.Context, ...libp2pm.Option) (host.Host, error)) Options {
	return Options{
		hostFactory: factory,
	}
}

var UserAgent = userAgent
