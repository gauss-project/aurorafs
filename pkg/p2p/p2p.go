// Package p2p provides the peer-to-peer abstractions used
// across different protocols in Aurora.
package p2p

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"github.com/libp2p/go-libp2p-core/network"
	libp2ppeer "github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// ReachabilityStatus represents the node reachability status.
type ReachabilityStatus network.Reachability

// String implements the fmt.Stringer interface.
func (rs ReachabilityStatus) String() string {
	return network.Reachability(rs).String()
}

const (
	ReachabilityStatusUnknown = ReachabilityStatus(network.ReachabilityUnknown)
	ReachabilityStatusPublic  = ReachabilityStatus(network.ReachabilityPublic)
	ReachabilityStatusPrivate = ReachabilityStatus(network.ReachabilityPrivate)
)

// Service provides methods to handle p2p Peers and Protocols.
type Service interface {
	CallHandler(ctx context.Context, last Peer, stream Stream) (relayData *pb.RouteRelayReq, w WriterChan, r ReaderChan, forward bool, err error)
	AddProtocol(ProtocolSpec) error
	Connect
	Disconnecter
	Peers() []Peer
	PeerID(overlay boson.Address) (id libp2ppeer.ID, found bool)
	ResourceManager() network.ResourceManager
	BlocklistedPeers() ([]BlockPeers, error)
	BlocklistRemove(overlay boson.Address) error
	Addresses() ([]ma.Multiaddr, error)
	NATAddresses() ([]net.Addr, error)
	SetPickyNotifier(PickyNotifier)
	Halter
}

type Connect interface {
	// Connect to a peer but do not notify topology about the established connection.
	Connect(ctx context.Context, addr ma.Multiaddr) (peer *Peer, err error)
}

type Disconnecter interface {
	Disconnect(overlay boson.Address, reason string) error
	Blocklister
}

type Blocklister interface {
	// Blocklist will disconnect a peer and put it on a blocklist (blocking in & out connections) for provided duration
	// Duration 0 is treated as an infinite duration.
	Blocklist(overlay boson.Address, duration time.Duration, reason string) error
}

type Halter interface {
	// Halt new incoming connections while shutting down
	Halt()
}

// PickyNotifier can decide whether a peer should be picked
type PickyNotifier interface {
	Picker
	Notifier
	ReachabilityUpdater
	ReachableNotifier
}

type Picker interface {
	Pick(Peer) bool
}

type ReachableNotifier interface {
	Reachable(boson.Address, ReachabilityStatus)
}

type Reacher interface {
	Connected(boson.Address, ma.Multiaddr)
	Disconnected(boson.Address)
}

type ReachabilityUpdater interface {
	UpdateReachability(ReachabilityStatus)
}

type Notifier interface {
	Connected(context.Context, Peer, bool) error
	Disconnected(peer Peer, reason string)
	Announce(ctx context.Context, peer boson.Address, fullnode bool) error
	AnnounceTo(ctx context.Context, addressee, peer boson.Address, fullnode bool) error
	NotifyPeerState(peer PeerInfo)
}

// DebugService extends the Service with method used for debugging.
type DebugService interface {
	Service
	SetWelcomeMessage(val string) error
	GetWelcomeMessage() string
}

// Streamer is able to create a new Stream.
type Streamer interface {
	NewStream(ctx context.Context, address boson.Address, h Headers, protocol, version, stream string) (Stream, error)
	NewRelayStream(ctx context.Context, address boson.Address, h Headers, protocol, version, stream string, midCall bool) (Stream, error)
}

type StreamerDisconnecter interface {
	Streamer
	Disconnecter
}

// Pinger interface is used to ping a underlay address which is not yet known to the aurora node.
// It uses libp2p's default ping protocol. This is different from the PingPong protocol as this
// is meant to be used before we know a particular underlay and we can consider it useful
type Pinger interface {
	Ping(ctx context.Context, addr ma.Multiaddr) (rtt time.Duration, err error)
}

type StreamerPinger interface {
	Streamer
	Pinger
}

// Stream represent a bidirectional data Stream.
type Stream interface {
	io.ReadWriter
	io.Closer
	ResponseHeaders() Headers
	Headers() Headers
	FullClose() error
	Reset() error
}

// ProtocolSpec defines a collection of Stream specifications with handlers.
type ProtocolSpec struct {
	Name          string
	Version       string
	StreamSpecs   []StreamSpec
	ConnectIn     func(context.Context, Peer) error
	ConnectOut    func(context.Context, Peer) error
	DisconnectIn  func(Peer) error
	DisconnectOut func(Peer) error
}

// StreamSpec defines a Stream handling within the protocol.
type StreamSpec struct {
	Name    string
	Handler HandlerFunc
	Headler HeadlerFunc
}

type BlockPeers struct {
	Address   boson.Address `json:"address"`
	Timestamp string        `json:"timestamp"`
	Duration  float64       `json:"duration"`
}

// Peer holds information about a Peer.
type Peer struct {
	Address boson.Address `json:"address"`
	Mode    aurora.Model  `json:"mode"`
}

type PeerInfo struct {
	Overlay boson.Address `json:"overlay"`
	Mode    []byte        `json:"mode"`
	State   PeerState     `json:"state"`
	Reason  string        `json:"reason"`
}

type WriterChan struct {
	W   chan []byte
	Err chan error
}

type ReaderChan struct {
	R   chan []byte
	Err chan error
}

// HandlerFunc handles a received Stream from a Peer.
type HandlerFunc func(context.Context, Peer, Stream) error

// HandlerMiddleware decorates a HandlerFunc by returning a new one.
type HandlerMiddleware func(HandlerFunc) HandlerFunc

// HeadlerFunc is returning response headers based on the received request
// headers.
type HeadlerFunc func(Headers, boson.Address) Headers

// Headers represents a collection of p2p header key value pairs.
type Headers map[string][]byte

// Common header names.
const (
	HeaderNameTracingSpanContext = "tracing-span-context"
)

// NewAuroraStreamName constructs a libp2p compatible stream name out of
// protocol name and version and stream name.
func NewAuroraStreamName(protocol, version, stream string) string {
	return "/boson/" + protocol + "/" + version + "/" + stream
}

type PeerState int

const (
	PeerStateConnectIn PeerState = iota + 1
	PeerStateConnectOut
	PeerStateDisconnect
)
