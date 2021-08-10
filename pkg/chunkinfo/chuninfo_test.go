package chunk_info

import (
	"fmt"
	"testing"
)

func TestFindChunkInfo(t *testing.T) {
	ci := New()
	// 发起金字塔req
	ci.FindChunkInfo(nil, "1", []string{"a", "b"})
	// 模拟金字塔resp
	res := make(map[string][]string)
	res["2"] = []string{"c", "d", "d"}
	res["3"] = []string{"e", "d"}
	py := make(map[string]map[string]uint)
	s := make(map[string]uint)
	s["2"] = 0
	s["3"] = 1
	py["1"] = s
	resp := ci.ct.createChunkPyramidResp("1", py, res)
	ci.onChunkInfoHandle(nil, "resp/chunkpyramid", "a", resp)
	q := ci.queues["1"]
	if q.len(Pulling) != 4 {
		t.Fatal()
	}
	pyramid := ci.GetChunkPyramid("1")
	if pyramid == nil {
		t.Fatal()
	}
	nodes := ci.GetChunkInfo("1", "2")
	for _, n := range nodes {
		fmt.Printf("%s \n", n)
	}
	if len(nodes) != 2 {
		t.Fatal()
	}
	// 收到req请求处理
	ci.OnChunkTransferred("2", "1", "c")
	req := ci.cd.createChunkInfoReq("1")
	ci.onChunkInfoHandle(nil, "req/chunkinfo", "a", req)
	// req请求金字塔 金字塔获取为提供无法测试
}

func TestGetChunkInfo(t *testing.T) {

}
func TestGetChunkPyramid(t *testing.T) {

}

func TestCancelFindChunkInfo(t *testing.T) {

}

func TestOnChunkTransferred(t *testing.T) {

}
