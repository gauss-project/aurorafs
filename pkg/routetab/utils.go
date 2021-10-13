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

func GetClosestNeighborLimit(target boson.Address, routes []*Path, limit int) (out []boson.Address) {
	has := make(map[string]bool)
	for _, path := range routes {
		for k, v := range path.Item {
			if v.Equal(target) {
				if k-1 > 0 {
					has[path.Item[k-1].String()] = true
				}
				if k+1 < len(path.Item) {
					has[path.Item[k+1].String()] = true
				}
				break
			}
		}
		if len(has) >= limit {
			break
		}
	}
	for hex := range has {
		out = append(out, boson.MustParseHexAddress(hex))
	}
	return
}
