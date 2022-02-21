package multicast

import (
	"context"
	"github.com/gauss-project/aurorafs/pkg/aurora"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/multicast/pb"
)

type GroupInterface interface {
	Multicast(info *pb.MulticastMsg, skip ...boson.Address) error
	ObserveGroup(gid boson.Address, peers ...boson.Address) error
	ObserveGroupCancel(gid boson.Address) error
	JoinGroup(ctx context.Context, gid boson.Address, ch chan Message, peers ...boson.Address) error
	LeaveGroup(gid boson.Address) error
	Snapshot() *model.KadParams
	StartDiscover()
	SubscribeLogContent() (c <-chan LogContent, unsubscribe func())
	SubscribeMulticastMsg(gid boson.Address) (c <-chan Message, unsubscribe func(), err error)
	GetMulticastNode(groupName string) (peer boson.Address, err error)
	AddGroup(ctx context.Context, groups []aurora.ConfigNodeGroup)
}

type Message struct {
	ID         uint64
	CreateTime int64
	GID        boson.Address
	Origin     boson.Address
	Data       []byte
	From       boson.Address
}

type LogContent struct {
	Event string
	Time  int64 // ms
	Data  Message
}
