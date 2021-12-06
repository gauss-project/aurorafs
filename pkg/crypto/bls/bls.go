package bls

import (
	"crypto/sha256"
)

func Sign(body []byte, sign ...[]byte) []byte {
	for _, v := range sign {
		body = append(body, v...)
	}
	hash := sha256.Sum256(body)
	return hash[:]
}
