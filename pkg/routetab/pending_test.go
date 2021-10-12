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

	dest := test.RandomAddress()
	src := test.RandomAddress()
	src1 := test.RandomAddress()
	p.Add(dest, src, nil)
	p.Add(dest, src1, nil)

	res := p.Get(dest)
	if len(res) != 2 {
		t.Fatalf("expect find res len 2,got %d", len(res))
	}
	exp := []boson.Address{src, src1}
	for k, v := range res {
		if !v.Src.Equal(exp[k]) {
			t.Fatalf("expect res[%d] src %s, got %s", k, exp[k].String(), v.Src.String())
		}
	}
	time.Sleep(time.Millisecond * 100)
	src2 := test.RandomAddress()
	p.Add(dest, src2, nil)

	p.Gc(time.Millisecond * 100)
	res2 := p.Get(dest)
	if len(res2) != 1 {
		t.Fatalf("expect find res2 len 1,got %d", len(res2))
	}
	if !res2[0].Src.Equal(src2) {
		t.Fatalf("expect res2[0] src %s, got %s", src2.String(), res2[0].Src.String())
	}
	p.Add(dest, src1, nil)
	p.Delete(dest)
	res3 := p.Get(dest)
	if res3 != nil {
		t.Fatalf("expect res3 is nil")
	}
}
