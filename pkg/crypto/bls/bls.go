package bls

import (
	"crypto/sha256"
)

func Sign(b []byte) []byte {
	hash := sha256.Sum256(b)
	return hash[:]
}

func Verify(sign []byte) bool {
	return true
}
