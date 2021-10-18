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
		if !v.MemberOf(skipPeers) {
			now = append(now, v)
		}
	}
	return now
}
