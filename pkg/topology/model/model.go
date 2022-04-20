package model

import (
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
)

// PeerInfo is a view of peer information exposed to a user.
type PeerInfo struct {
	Address boson.Address       `json:"address"`
	Metrics *MetricSnapshotView `json:"metrics,omitempty"`
}

const (
	PeerConnectionDirectionInbound  PeerConnectionDirection = "inbound"
	PeerConnectionDirectionOutbound PeerConnectionDirection = "outbound"
)

// PeerConnectionDirection represents peer connection direction.
type PeerConnectionDirection string

// Snapshot represents a snapshot of peers' metrics counters.
type Snapshot struct {
	LastSeenTimestamp          int64                   `json:"lastSeenTimestamp"`
	SessionConnectionRetry     uint64                  `json:"sessionConnectionRetry"`
	ConnectionTotalDuration    time.Duration           `json:"connectionTotalDuration"`
	SessionConnectionDuration  time.Duration           `json:"sessionConnectionDuration"`
	SessionConnectionDirection PeerConnectionDirection `json:"sessionConnectionDirection"`
	LatencyEWMA                time.Duration           `json:"latencyEWMA"`
	Reachability               p2p.ReachabilityStatus  `json:"reachability"`
}

// HasAtMaxOneConnectionAttempt returns true if the snapshot represents a new
// peer which has at maximum one session connection attempt but it still isn't
// logged in.
func (ss *Snapshot) HasAtMaxOneConnectionAttempt() bool {
	return ss.LastSeenTimestamp == 0 && ss.SessionConnectionRetry <= 1
}

// MetricSnapshotView represents snapshot of metrics counters in more human readable form.
type MetricSnapshotView struct {
	LastSeenTimestamp          int64   `json:"lastSeenTimestamp"`
	SessionConnectionRetry     uint64  `json:"sessionConnectionRetry"`
	ConnectionTotalDuration    float64 `json:"connectionTotalDuration"`
	SessionConnectionDuration  float64 `json:"sessionConnectionDuration"`
	SessionConnectionDirection string  `json:"sessionConnectionDirection"`
	LatencyEWMA                int64   `json:"latencyEWMA"`
	Reachability               string  `json:"reachability"`
}

type BinInfo struct {
	BinPopulation     uint        `json:"population"`
	BinConnected      uint        `json:"connected"`
	DisconnectedPeers []*PeerInfo `json:"disconnectedPeers"`
	ConnectedPeers    []*PeerInfo `json:"connectedPeers"`
}

type KadBins struct {
	Bin0  BinInfo `json:"bin_0"`
	Bin1  BinInfo `json:"bin_1"`
	Bin2  BinInfo `json:"bin_2"`
	Bin3  BinInfo `json:"bin_3"`
	Bin4  BinInfo `json:"bin_4"`
	Bin5  BinInfo `json:"bin_5"`
	Bin6  BinInfo `json:"bin_6"`
	Bin7  BinInfo `json:"bin_7"`
	Bin8  BinInfo `json:"bin_8"`
	Bin9  BinInfo `json:"bin_9"`
	Bin10 BinInfo `json:"bin_10"`
	Bin11 BinInfo `json:"bin_11"`
	Bin12 BinInfo `json:"bin_12"`
	Bin13 BinInfo `json:"bin_13"`
	Bin14 BinInfo `json:"bin_14"`
	Bin15 BinInfo `json:"bin_15"`
	Bin16 BinInfo `json:"bin_16"`
	Bin17 BinInfo `json:"bin_17"`
	Bin18 BinInfo `json:"bin_18"`
	Bin19 BinInfo `json:"bin_19"`
	Bin20 BinInfo `json:"bin_20"`
	Bin21 BinInfo `json:"bin_21"`
	Bin22 BinInfo `json:"bin_22"`
	Bin23 BinInfo `json:"bin_23"`
	Bin24 BinInfo `json:"bin_24"`
	Bin25 BinInfo `json:"bin_25"`
	Bin26 BinInfo `json:"bin_26"`
	Bin27 BinInfo `json:"bin_27"`
	Bin28 BinInfo `json:"bin_28"`
	Bin29 BinInfo `json:"bin_29"`
	Bin30 BinInfo `json:"bin_30"`
	Bin31 BinInfo `json:"bin_31"`
}

type KadParams struct {
	Base           string    `json:"baseAddr"`       // base address string
	Population     int       `json:"population"`     // known
	Connected      int       `json:"connected"`      // connected count
	Timestamp      time.Time `json:"timestamp"`      // now
	NNLowWatermark int       `json:"nnLowWatermark"` // low watermark for depth calculation
	Depth          uint8     `json:"depth"`          // current depth
	Reachability   string    `json:"reachability"`   // current reachability status
	Bins           KadBins   `json:"bins"`           // individual bin info
	LightNodes     BinInfo   `json:"lightNodes"`     // light nodes bin info
	BootNodes      BinInfo   `json:"bootNodes"`      // boot nodes bin info
}

// EachPeerFunc is a callback that is called with a peer and its PO
type EachPeerFunc func(boson.Address, uint8) (stop, jumpToNext bool, err error)
