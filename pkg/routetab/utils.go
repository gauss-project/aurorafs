package routetab

import (
	"bytes"
	"crypto/sha256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
)

func inPath(b []byte, path [][]byte) bool {
	for _, v := range path {
		if bytes.Equal(v, b) {
			return true
		}
	}
	return false
}

func inPaths(b [][]byte, path []boson.Address) bool {
	for _, v := range path {
		for _, v1 := range b {
			if bytes.Equal(v.Bytes(), v1) {
				return true
			}
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

func existRoute(route TargetRoute, routes []TargetRoute) bool {
	for _, v := range routes {
		if v.PathKey == route.PathKey && v.Neighbor.Equal(route.Neighbor) {
			return true
		}
	}
	return false
}

func generatePathItems(paths [][]byte) (pathKey common.Hash, items []boson.Address) {
	s := make([]byte, 0)
	for _, v := range paths {
		s = append(s, v...)
		items = append(items, boson.NewAddress(v))
	}
	pathKey = sha256.Sum256(s)
	return
}

func convItemsToBytes(items []boson.Address) (paths [][]byte) {
	for _, v := range items {
		paths = append(paths, v.Bytes())
	}
	return
}

func verifyPath(path *pb.Path) bool {
	return true
}

func getTargetKey(target boson.Address) common.Hash {
	return common.BytesToHash(target.Bytes())
}

func getPendingReqKey(target, next boson.Address) string {
	return target.String() + next.String()
}
