package routetab_test

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/routetab"
	"testing"
	"time"
)

func TestPendCallResTab(t *testing.T) {
	p := routetab.NewPendCallResTab()

	next := test.RandomAddress()
	dest := test.RandomAddress()
	src := test.RandomAddress()
	src1 := test.RandomAddress()
	has := p.Add(dest, src, next, nil)
	if has {
		t.Fatalf("expect not has req log")
	}
	has = p.Add(dest, src1, next, nil)
	if !has {
		t.Fatalf("expect has req log")
	}
	checkReqLog := func() {
		cnt := 0
		p.ReqLogRange(func(key, value interface{}) bool {
			cnt++
			if dest.ByteString()+next.ByteString() != key.(string) {
				t.Fatalf("expect relog equal dest")
			}
			return true
		})
		if cnt != 1 {
			t.Fatalf("expect relog len 1,got %d", cnt)
		}
	}
	checkReqLog()
	has = p.Add(dest, src1, next, nil)
	if !has {
		t.Fatalf("expect has req log")
	}
	checkReqLog()

	res := p.Get(dest, next)
	if len(res) != 3 {
		t.Fatalf("expect find res len 3,got %d", len(res))
	}
	exp := []boson.Address{src, src1, src1}
	for k, v := range res {
		if !v.Src.Equal(exp[k]) {
			t.Fatalf("expect res[%d] src %s, got %s", k, exp[k].String(), v.Src.String())
		}
	}
	time.Sleep(time.Millisecond * 100)
	src2 := test.RandomAddress()
	has = p.Add(dest, src2, next, nil)
	if has {
		t.Fatalf("expect no has req log")
	}
	p.GcResItems(time.Millisecond * 100)
	p.GcReqLog(time.Millisecond * 100)
	res2 := p.Get(dest, next)
	if len(res2) != 1 {
		t.Fatalf("expect find res2 len 1,got %d", len(res2))
	}
	if !res2[0].Src.Equal(src2) {
		t.Fatalf("expect res2[0] src %s, got %s", src2.String(), res2[0].Src.String())
	}
	p.Add(dest, src1, next, nil)
	p.Delete(dest, next)
	res3 := p.Get(dest, next)
	if res3 != nil {
		t.Fatalf("expect res3 is nil")
	}
}
