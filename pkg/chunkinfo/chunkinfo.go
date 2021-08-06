package chunk_info

import (
	cid "github.com/ethersphere/bee/pkg/chunkinfo/discover"
	cipd "github.com/ethersphere/bee/pkg/chunkinfo/pending"
	cip "github.com/ethersphere/bee/pkg/chunkinfo/pyramid"
	citn "github.com/ethersphere/bee/pkg/chunkinfo/tabneighbor"
)

type Chunkinfo struct {
	ct  *citn.ChunkInfoTabNeighbor
	cd  *cid.ChunkInfoDiscover
	cp  *cip.ChunkPyramid
	cpd *cipd.PendingFinderInfo
}

func New() *Chunkinfo {
	cd := cid.New()
	ct := citn.New()
	cp := cip.New()
	cpd := cipd.New()
	return &Chunkinfo{ct: ct, cd: cd, cp: cp, cpd: cpd}
}

func (ci *Chunkinfo) FindChunkInfo(authInfo []byte, rootCid string, nodes []string) {
	// 获取金字塔 如果已经存在金字塔直接发起doFindChunkInfo
	ci.cp.DoFindChunkPyramid(authInfo, rootCid, nodes)
}
func (ci *Chunkinfo) onFindChunkPyramid(rootCid string, pyramids map[string]map[string]uint) {
	// 是否已经发现了
	_, ok := ci.cp.Pyramid[rootCid]
	if !ok {
		return
	}
	// todo 验证金字塔数据是否正确
	// 更新chunkpyramid
	ci.cp.UpdateChunkPyramid(pyramids)
	// 调用onFindChunkInfo
}

func (ci *Chunkinfo) onFindChunkInfo(rootCid string, pyramids map[string][]string) {
	// 检查是否有新节点
	// 更新chunkinfodiscover
	ci.cd.UpdateChunkInfo(rootCid, pyramids)
	// 更新pull
	// 发送新节点通知
}
