package routetab_test

import (
	"bytes"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/crypto/bls"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	mockstate "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"testing"
	"time"
)

func TestTable_ReqToResp(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	req := &pb.RouteReq{
		Dest:  self.Bytes(),
		Alpha: 1,
		Path: &pb.Path{
			Sign: p0.Bytes(),
			Item: reqItem,
		},
	}
	t.Run("test dest is self", func(t *testing.T) {
		resp := table.ReqToResp(req, nil)

		if !bytes.Equal(resp.Dest, req.Dest) {
			t.Fatal("req dest not mate")
		}
		if len(resp.Paths) != 1 {
			t.Fatalf("resp paths len expect 1, got %d", len(resp.Paths))
		}
		item := resp.Paths[0].Item
		if len(item) != 4 {
			t.Fatalf("resp path.item len expect 4, got %d", len(item))
		}

		expectItem := append(reqItem, self.Bytes())
		for k, v := range item {
			if !bytes.Equal(v, expectItem[k]) {
				t.Fatalf("resp item k=%d ,not mate", k)
			}
		}
	})

	t.Run("test dest exists in route table,and resp to save", func(t *testing.T) {
		r1 := test.RandomAddress()
		r2 := test.RandomAddress()
		r3 := test.RandomAddress()
		r4 := test.RandomAddress()
		dest := test.RandomAddress()

		paths := []*routetab.Path{
			{
				CreateTime: 0,
				Sign:       r1.Bytes(),
				Item:       []boson.Address{self, r1, r2, dest},
				UsedTime:   time.Time{},
			},
			{
				CreateTime: 0,
				Sign:       r3.Bytes(),
				Item:       []boson.Address{self, r3, r4, dest},
				UsedTime:   time.Time{},
			},
		}

		resp := table.ReqToResp(req, paths)

		if !bytes.Equal(resp.Dest, req.Dest) {
			t.Fatal("req dest not mate")
		}
		if len(resp.Paths) != 2 {
			t.Fatalf("resp paths len expect 2, got %d", len(resp.Paths))
		}
		items := [][][]byte{
			{self.Bytes(), r1.Bytes(), r2.Bytes(), dest.Bytes()},
			{self.Bytes(), r3.Bytes(), r4.Bytes(), dest.Bytes()},
		}

		for k, v := range resp.Paths {
			if len(v.Item) != 7 {
				t.Fatalf("resp path[%d].item len expect 7, got %d", k, len(v.Item))
			}
			expectItem := append(reqItem, items[k]...)
			for kk, vv := range v.Item {
				if !bytes.Equal(vv, expectItem[kk]) {
					t.Fatalf("resp item kk=%d ,not mate", kk)
				}
			}
		}
		err := table.RespToSave(resp)
		if err != nil {
			t.Fatal(err)
		}
		destRoute, err := table.Get(dest)
		if err != nil {
			t.Fatal(err)
		}
		if len(destRoute) != 2 {
			t.Fatalf("destRoute paths len expect 2, got %d", len(destRoute))
		}
		expectItem := [][]byte{self.Bytes(), r1.Bytes(), r2.Bytes(), dest.Bytes()}
		expectItem1 := [][]byte{self.Bytes(), r3.Bytes(), r4.Bytes(), dest.Bytes()}

		checkPath(t, "destRoute", 4, expectItem, destRoute[0])
		checkPath(t, "destRoute1", 4, expectItem1, destRoute[1])

		r2Route, err := table.Get(r2)
		if err != nil {
			t.Fatal(err)
		}
		if len(r2Route) != 1 {
			t.Fatalf("r2Route paths len expect 1, got %d", len(r2Route))
		}
		checkPath(t, "r2Route", 4, expectItem, r2Route[0])
		r4Route, err := table.Get(r4)
		if err != nil {
			t.Fatal(err)
		}
		if len(r4Route) != 1 {
			t.Fatalf("r4Route paths len expect 1, got %d", len(r4Route))
		}
		checkPath(t, "r4Route", 4, expectItem1, r4Route[0])
		_, err = table.Get(r3)
		if !errors.Is(err, routetab.ErrNotFound) {
			t.Fatalf("expect r4Route notfound")
		}
	})

}

func checkPath(t *testing.T, name string, expectLen int, expectItem [][]byte, path *routetab.Path) {
	if len(path.Item) != expectLen {
		t.Fatalf("%s path.item len expect %d, got %d", name, expectLen, len(path.Item))
	}

	for k, v := range path.Item {
		if !bytes.Equal(v.Bytes(), expectItem[k]) {
			t.Fatalf("%s path.item kk=%d ,not mate", name, k)
		}
	}
}

func TestTable_ReqToSave(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	req := &pb.RouteReq{
		Dest:  self.Bytes(),
		Alpha: 1,
		Path: &pb.Path{
			Sign: p0.Bytes(),
			Item: reqItem,
		},
	}
	err := table.ReqToSave(req)
	if err != nil {
		t.Fatal(err)
	}
	// p0 dest
	p0route, err := table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p0route) != 1 {
		t.Fatalf("p0 paths len expect 1, got %d", len(p0route))
	}
	expectItem := [][]byte{self.Bytes(), p2.Bytes(), p1.Bytes(), p0.Bytes()}
	checkPath(t, "p0", 4, expectItem, p0route[0])
	// p1 dest
	p1route, err := table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
	if len(p1route) != 1 {
		t.Fatalf("po paths len expect 1, got %d", len(p1route))
	}
	checkPath(t, "p1", 4, expectItem, p1route[0])
	// p2 dest
	_, err = table.Get(p2)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect dest p2 route notfound")
	}
	// get next hop
	p1Next := table.GetNextHop(p1, bls.Sign(p0.Bytes()))
	p0Next := table.GetNextHop(p0, bls.Sign(p0.Bytes()))
	if !p1Next.Equal(p0Next) {
		t.Fatalf("expect p0,p1 next hop equal")
	}
	if !p1Next.Equal(p2) {
		t.Fatalf("expect p1 next equal p2")
	}
}

func TestTable_Gc(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	req := &pb.RouteReq{
		Dest:  self.Bytes(),
		Alpha: 1,
		Path: &pb.Path{
			Sign: p0.Bytes(),
			Item: reqItem,
		},
	}
	err := table.ReqToSave(req)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
	p0Next := table.GetNextHop(p0, bls.Sign(p0.Bytes()))
	if !p0Next.Equal(p2) {
		t.Fatalf("expect p1 next equal p2")
	}
	time.Sleep(time.Millisecond * 500)
	table.Gc(time.Millisecond * 300)
	_, err = table.Get(p0)
	if !errors.Is(err, routetab.ErrNotFound) {
		t.Fatalf("expect p0 route not found")
	}
	p0Next = table.GetNextHop(p0, bls.Sign(p0.Bytes()))
	if p0Next.Equal(p2) {
		t.Fatalf("expect p0 next not equal p2")
	}
	p1Next := table.GetNextHop(p1, bls.Sign(p0.Bytes()))
	if !p1Next.IsZero() {
		t.Fatalf("expect p1 next is zero")
	}
}

func TestTable_Resume(t *testing.T) {
	self := test.RandomAddress()
	table := routetab.NewRouteTable(self, mockstate.NewStateStore())

	p0 := test.RandomAddress()
	p1 := test.RandomAddress()
	p2 := test.RandomAddress()

	reqItem := [][]byte{p0.Bytes(), p1.Bytes(), p2.Bytes()}
	req := &pb.RouteReq{
		Dest:  self.Bytes(),
		Alpha: 1,
		Path: &pb.Path{
			Sign: p0.Bytes(),
			Item: reqItem,
		},
	}
	err := table.ReqToSave(req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p0)
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

	table.ResumeSigned()
	table.ResumeRoute()

	_, err = table.Get(p0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.Get(p1)
	if err != nil {
		t.Fatal(err)
	}
}
