package chunk_info

import "testing"

func TestFindChunkInfo(t *testing.T) {
	ci := New()
	ci.FindChunkInfo(nil, "1", []string{"a", "b"})
	// 模拟树resp
	go func() {
		res := make(map[string][]string)
		res["2"] = []string{"c", "d"}
		res["3"] = []string{"e"}
		py := make(map[string]map[string]uint)
		s := make(map[string]uint)
		s["2"] = 0
		s["3"] = 1
		py["1"] = s
		resp := ci.ct.createChunkPyramidResp("1", py, res)
		ci.onChunkInfoHandle(nil, "resp/chunkpyramid", "a", resp)
	}()
	// 模拟不停发现resp
}

func TestGetChunkInfo(t *testing.T) {

}
func TestGetChunkPyramid(t *testing.T) {

}

func TestCancelFindChunkInfo(t *testing.T) {

}

func TestOnChunkTransferred(t *testing.T) {

}
