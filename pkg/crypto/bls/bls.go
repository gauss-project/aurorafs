package bls

import (
	"crypto/sha256"
	"time"
)

func Sign(b []byte) []byte {
	// todo Avoid duplicate routing signatures temporarily
	s := time.Now().AppendFormat(b, "2006/01/02 15:04:05 ")
	hash := sha256.Sum256(s)
	return hash[:]
}

func Verify(sign []byte) bool {
	return true
}
