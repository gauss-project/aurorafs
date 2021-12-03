package routetab_test

import (
	"bytes"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"testing"
	"time"
)

func TestTable_PathsToSave(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	Paths := []*pb.Path{{
		Items: reqItem,
	}}
	table.SavePaths(Paths)

	paths, err := table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) != 1 {
		t.Fatalf("paths len expect 1, got %d", len(paths))
	}
	item := paths[0].Items
	if len(item) != 3 {
		t.Fatalf("path.item len expect 3, got %d", len(item))
	}

	for k, v := range item {
		if !bytes.Equal(v.Bytes(), reqItem[k]) {
			t.Fatalf("item k=%d ,not mate", k)
		}
	}
	paths1, err := table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != len(paths1) || len(paths1) != 1 {
		t.Fatalf("expect paths,paths1 length equal 1")
	}
	for k, v1 := range paths[0].Items {
		if !paths1[0].Items[k].Equal(v1) {
			t.Fatalf("expect paths,paths1 items k=%d equal", k)
		}
	}
}

func TestTable_Gc(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	Paths := []*pb.Path{{
		Items: reqItem,
	}}
	table.SavePaths(Paths)

	_, err := table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
	list := table.GetNextHop(p0)
	if len(list) != 1 || !list[0].Equal(p2) {
		t.Fatalf("expect length=1 and  p0 next equal p2")
	}
	list = table.GetNextHop(p1)
	if len(list) != 1 || !list[0].Equal(p2) {
		t.Fatalf("expect length=1 and  p1 next equal p2")
	}

	time.Sleep(time.Millisecond * 500)
	table.Gc(time.Millisecond * 300)
	_, err = table.Get(p0)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect p0 route not found")
	}
	list2 := table.GetNextHop(p0)
	if len(list2) != 0 {
		t.Fatalf("expect p0 next length=0")
	}
	list2 = table.GetNextHop(p1)
	if len(list2) != 0 {
		t.Fatalf("expect p2 next length=0")
	}
}

func TestTable_Resume(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	Paths := []*pb.Path{{
		Items: reqItem,
	}}
	table.SavePaths(Paths)

	_, err := table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}

	table.TableClean()
	_, err = table.Get(p0)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect p0 route not found")
	}

	table.ResumeRoutes()
	table.ResumePaths()

	_, err = table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
}
