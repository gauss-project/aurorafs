package bls

import (
	"crypto/sha256"

	"github.com/gogf/gf/v2/util/gconv"
	bls12381 "github.com/kilic/bls12-381"
)

func G1Generator(ctime int64) *bls12381.PointG1 {
	b := gconv.Bytes(ctime)
	one, _ := bls12381.NewG1().FromBytes(b)
	return one
}

func Hash256(m []byte) []byte {
	hash := sha256.Sum256(m)
	return hash[:]
}
