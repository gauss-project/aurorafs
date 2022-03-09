package multicast

import (
	"crypto/sha256"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gogf/gf/v2/os/gcache"
	"github.com/gogf/gf/v2/os/gctx"
)

var (
	cacheCtx = gctx.New()
	cache    = gcache.New()
)

func GenerateGID(name string) boson.Address {
	b := sha256.Sum256([]byte(name))
	return boson.NewAddress(b[:])
}

func RandomPeer(peers []boson.Address) boson.Address {
	if len(peers) == 0 {
		return boson.ZeroAddress
	}
	rnd := rand.Intn(len(peers))
	return peers[rnd]
}

func RandomPeersLimit(peers []boson.Address, limit int) []boson.Address {
	if limit <= 1 || len(peers) <= limit {
		return peers
	}
	tmpOrigin := make([]boson.Address, len(peers))
	copy(tmpOrigin, peers)

	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(tmpOrigin), func(i int, j int) {
		tmpOrigin[i], tmpOrigin[j] = tmpOrigin[j], tmpOrigin[i]
	})

	result := make([]boson.Address, 0, limit)
	for index, value := range tmpOrigin {
		if index == limit {
			break
		}
		result = append(result, value)
	}
	return result
}

func ConvertHashToGID(h common.Hash) boson.Address {
	s := h.Hex()[2:]
	return boson.MustParseHexAddress(s)
}
