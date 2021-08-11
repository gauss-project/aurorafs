package chunkinfo

import (
	"fmt"
	"testing"
	"time"
)

func TestChunkPyramid(t *testing.T) {
	ci := New(nil, nil, nil)
	// mock pyramid req
	ci.FindChunkInfo(nil, "1", []string{"a", "b"})
	ci.CancelFindChunkInfo("1")
	// mock pyramid resp
	go func() {
		res := make(map[string][]string)
		res["4"] = []string{"c", "d", "d"}
		res["3"] = []string{"e", "d"}
		py := make(map[string]map[string]uint)
		s := make(map[string]uint)
		s1 := make(map[string]uint)
		s["2"] = 0
		s["3"] = 1
		s1["4"] = 0
		py["1"] = s
		py["2"] = s1
		resp := ci.ct.createChunkPyramidResp("1", py, res)
		ci.onChunkPyramidResp(nil, "a", resp)
	}()
	time.Sleep(5 * time.Second)
	q := ci.queues["1"]
	if q.len(UnPull) != 3 {
		t.Fatal()
	}
	pyramid := ci.GetChunkPyramid("1")
	if pyramid == nil {
		t.Fatal()
	}
	nodes := ci.GetChunkInfo("1", "4")
	for _, n := range nodes {
		fmt.Printf("%s \n", n)
	}
	if len(nodes) != 2 {
		t.Fatal()
	}
}

func TestChunkInfo(t *testing.T) {
	ci := New(nil, nil, nil)
	// handle chunkinfo req
	ci.OnChunkTransferred("2", "1", "c")
	req := ci.cd.createChunkInfoReq("1")
	ci.onChunkInfoReq(nil, "a", req)
}
