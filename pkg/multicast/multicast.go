package multicast

import (
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
)

type GroupInterface interface {
	Multicast(info *pb.MulticastMsg, skip ...boson.Address) error
	AddGroup(groups []model.ConfigNodeGroup) error
	RemoveGroup(gid boson.Address, gType model.GType) error
	Snapshot() *model.KadParams
	StartDiscover()
	SubscribeLogContent() (c <-chan LogContent, unsubscribe func())
	SubscribeMulticastMsg(gid boson.Address) (c <-chan Message, unsubscribe func(), err error)
	GetGroupPeers(groupName string) (out *GroupPeers, err error)
	GetMulticastNode(groupName string) (peer boson.Address, err error)
	SendMessage(ctx context.Context, data []byte, gid, dest boson.Address, tp SendOption) (out SendResult)
	SendReceive(ctx context.Context, data []byte, gid, dest boson.Address) (result []byte, err error)
	Send(ctx context.Context, data []byte, gid, dest boson.Address) (err error)
}

// Message multicast message
type Message struct {
	ID         uint64
	CreateTime int64
	GID        boson.Address
	Origin     boson.Address
	Data       []byte
	From       boson.Address
}

type GroupMessage struct {
	SessionID string
	GID       boson.Address
	Data      []byte
	From      boson.Address
}

type LogContent struct {
	Event string
	Time  int64 // ms
	Data  Message
}

type GroupPeers struct {
	Connected []boson.Address `json:"connected"`
	Keep      []boson.Address `json:"keep"`
}

type SendOption int

const (
	SendOnly SendOption = iota
	SendReceive
	SendStream
)

type SendResult struct {
	Read  chan []byte
	Write chan []byte
	Close chan struct{}
	ErrCh chan error
	Resp  []byte
	Err   error
}
