package libp2p

import rcmgr "github.com/libp2p/go-libp2p-resource-manager"

var (
	connFraction   = 10
	streamFraction = 8
)

func newLimier(o Options) rcmgr.Limiter {
	if o.LightNodeLimit < 1 {
		o.LightNodeLimit = 100
	}
	if o.KadBinMaxPeers < 1 {
		o.KadBinMaxPeers = 20
	}
	limit := rcmgr.DefaultLimits.WithSystemMemory(0.25, 128<<20, 8<<30)

	limit.SystemBaseLimit.ConnsOutbound = o.KadBinMaxPeers * connFraction
	limit.SystemBaseLimit.ConnsInbound = limit.SystemBaseLimit.ConnsOutbound/2 + o.LightNodeLimit
	limit.SystemBaseLimit.Conns = limit.SystemBaseLimit.ConnsOutbound + limit.SystemBaseLimit.ConnsInbound
	limit.SystemBaseLimit.FD = limit.SystemBaseLimit.Conns
	limit.SystemBaseLimit.StreamsInbound = limit.SystemBaseLimit.ConnsInbound * streamFraction
	limit.SystemBaseLimit.StreamsOutbound = limit.SystemBaseLimit.ConnsOutbound * streamFraction
	limit.SystemBaseLimit.Streams = limit.SystemBaseLimit.StreamsInbound + limit.SystemBaseLimit.StreamsOutbound

	limit.ServiceBaseLimit = limit.SystemBaseLimit
	limit.ServiceMemory = limit.SystemMemory
	limit.ProtocolBaseLimit = limit.SystemBaseLimit
	limit.ProtocolMemory = limit.SystemMemory

	limit.ServicePeerBaseLimit.StreamsInbound = 512
	limit.ServicePeerBaseLimit.StreamsOutbound = 512
	limit.ServicePeerBaseLimit.Streams = 1024
	limit.ServicePeerMemory.MemoryFraction *= 4
	limit.ServicePeerMemory.MinMemory = 32 << 20
	limit.ServicePeerMemory.MaxMemory = 256 << 20

	limit.ProtocolPeerBaseLimit = limit.ServicePeerBaseLimit
	limit.ProtocolPeerMemory = limit.ServicePeerMemory

	limit.PeerBaseLimit.StreamsInbound = 1024
	limit.PeerBaseLimit.StreamsOutbound = 1024
	limit.PeerBaseLimit.Streams = 2048
	limit.PeerMemory.MemoryFraction *= 4
	limit.PeerMemory.MinMemory = 64 << 20
	limit.PeerMemory.MaxMemory = 512 << 20

	return rcmgr.NewStaticLimiter(limit)
}
