package test

import (
	"fmt"
	"math/rand"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

// RandomAddressAt generates a random address
// at proximity order prox relative to address.
func RandomAddressAt(self boson.Address, prox int) boson.Address {
	addr := make([]byte, len(self.Bytes()))
	copy(addr, self.Bytes())
	pos := -1
	if prox >= 0 {
		pos = prox / 8
		trans := prox % 8
		transbytea := byte(0)
		for j := 0; j <= trans; j++ {
			transbytea |= 1 << uint8(7-j)
		}
		flipbyte := byte(1 << uint8(7-trans))
		transbyteb := transbytea ^ byte(255)
		randbyte := byte(rand.Intn(255))
		addr[pos] = ((addr[pos] & transbytea) ^ flipbyte) | randbyte&transbyteb
	}

	for i := pos + 1; i < len(addr); i++ {
		addr[i] = byte(rand.Intn(255))
	}

	a := boson.NewAddress(addr)
	if a.Equal(self) {
		panic(fmt.Sprint(a.String(), self.String()))
	}
	return a
}

// RandomAddress generates a random address.
func RandomAddress() boson.Address {
	b := make([]byte, 32)
	return RandomAddressAt(boson.NewAddress(b), -1)
}
