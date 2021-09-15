package mock

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
)

type MockPricer struct {
	peerPrice uint64
	price     uint64
}

func NewPricer(price, peerPrice uint64) *MockPricer {
	return &MockPricer{
		peerPrice: peerPrice,
		price:     price,
	}
}

func (pricer *MockPricer) PeerPrice(peer, chunk boson.Address) uint64 {
	return pricer.peerPrice
}

func (pricer *MockPricer) Price(chunk boson.Address) uint64 {
	return pricer.price
}
