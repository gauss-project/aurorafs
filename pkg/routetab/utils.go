package routetab

import (
	"bytes"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

func inPath(b []byte, path [][]byte) bool {
	for _, v := range path {
		if bytes.Equal(v, b) {
			return true
		}
	}
	return false
}

func skipPeers(src, skipPeers []boson.Address) []boson.Address {
	now := make([]boson.Address, 0)
	for _, v := range src {
		if !inAddress(v, skipPeers) {
			now = append(now, v)
		}
	}
	return now
}

func inAddress(src boson.Address, array []boson.Address) bool {
	for _, v := range array {
		if v.Equal(src) {
			return true
		}
	}
	return false
}

func GetClosestNeighborLimit(routes []*Path, limit int) (out []boson.Address) {
	has := make(map[string]bool)
	for _, v := range routes {
		has[v.Item[len(v.Item)-2].String()] = true
		if len(has) >= limit {
			break
		}
	}
	for hex := range has {
		out = append(out, boson.MustParseHexAddress(hex))
	}
	return
}
