package multicast

import (
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"io"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/aurora"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	mockRoute "github.com/gauss-project/aurorafs/pkg/routetab/mock"
	"github.com/gauss-project/aurorafs/pkg/topology/kademlia/mock"
	"github.com/sirupsen/logrus"
)

var (
	logger = logging.New(io.Discard, logrus.TraceLevel)
)

func TestGroupID(t *testing.T) {
	ids := []struct {
		name string
		hex  string
	}{
		{name: "group1", hex: "ec2825604d15b908e98d92defaefcd6600308baf498c435029d10fd9dae551c0"},
		{name: "gro 1", hex: "cc8a9d58bd1e8a774ead4ec5eedd6a875b8c50039e7d752a018249d047026d98"},
		{name: "gro ,l8+*", hex: "ee6a458a0312bf1618a3c75e5dc27e65380e3262bcd02b57e193e234a6b8d63a"},
		{name: "gro ,l8+dfsadhfaskdfhsdfddd", hex: "74f06c10a4802516accd6097d3fb455b0f341c8a7018430a32e67c2fead1c2eb"},
	}

	for _, v := range ids {
		gid := GenerateGID(v.name)
		if gid.String() != v.hex {
			t.Fatalf("%s hex expect %s got %s", v.name, v.hex, gid.String())
		}
		gh := common.BytesToHash(gid.Bytes())
		if ConvertHashToGID(gh).String() != v.hex {
			t.Fatalf("%s hex expect %s got %s", v.name, v.hex, gh.String())
		}
	}
}

func TestService_ObserveGroup(t *testing.T) {
	gid := GenerateGID("gid1")
	route := mockRoute.NewMockRouteTable()
	kad := mock.NewMockKademlia()
	s := NewService(test.RandomAddress(), aurora.NewModel(), nil, nil, kad, &route, logger, subscribe.NewSubPub(), Option{Dev: true})
	err := s.observeGroup(gid, model.ConfigNodeGroup{})
	if err != nil {
		t.Fatal(err)
	}
	err = s.RemoveGroup(gid, model.GTypeObserve)
	if err != nil {
		t.Fatal(err)
	}
}
