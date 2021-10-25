package mock

import (
	"context"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

var _ storage.Storer = (*MockStorer)(nil)

type MockStorer struct {
	store           map[string][]byte
	modePut         map[string]storage.ModePut
	modeSet         map[string]storage.ModeSet
	pinnedAddress   []boson.Address // Stores the pinned address
	pinnedCounter   []uint64        // and its respective counter. These are stored as slices to preserve the order.
	morePull        chan struct{}
	mtx             sync.Mutex
	quit            chan struct{}
	baseAddress     []byte
	bins            []uint64
}

func NewStorer(opts ...Option) *MockStorer {
	s := &MockStorer{
		store:    make(map[string][]byte),
		modePut:  make(map[string]storage.ModePut),
		modeSet:  make(map[string]storage.ModeSet),
		morePull: make(chan struct{}),
		quit:     make(chan struct{}),
		bins:     make([]uint64, boson.MaxBins),
	}

	for _, v := range opts {
		v.apply(s)
	}

	return s
}

func (m *MockStorer) Get(_ context.Context, _ storage.ModeGet, addr boson.Address) (ch boson.Chunk, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	v, has := m.store[addr.String()]
	if !has {
		return nil, storage.ErrNotFound
	}
	return boson.NewChunk(addr, v), nil
}

func (m *MockStorer) Put(ctx context.Context, mode storage.ModePut, chs ...boson.Chunk) (exist []bool, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	exist = make([]bool, len(chs))
	for i, ch := range chs {
		exist[i], err = m.has(ctx, storage.ModeHasChunk, ch.Address())
		if err != nil {
			return exist, err
		}
		if !exist[i] {
			po := boson.Proximity(ch.Address().Bytes(), m.baseAddress)
			m.bins[po]++
		}
		// this is needed since the chunk feeder shares memory across calls
		// to the pipeline. this is in order to avoid multiple allocations.
		// this change mimics the behavior of shed and localstore
		// and copies the data from the call into the in-memory store
		b := make([]byte, len(ch.Data()))
		copy(b, ch.Data())
		m.store[ch.Address().String()] = b
		m.modePut[ch.Address().String()] = mode

		// pin chunks if needed
		switch mode {
		case storage.ModePutUploadPin:
			// if mode is set pin, increment the pin counter
			var found bool
			addr := ch.Address()
			for i, ad := range m.pinnedAddress {
				if addr.String() == ad.String() {
					m.pinnedCounter[i] = m.pinnedCounter[i] + 1
					found = true
				}
			}
			if !found {
				m.pinnedAddress = append(m.pinnedAddress, addr)
				m.pinnedCounter = append(m.pinnedCounter, uint64(1))
			}
		default:
		}
	}
	return exist, nil
}

func (m *MockStorer) GetMulti(ctx context.Context, mode storage.ModeGet, addrs ...boson.Address) (ch []boson.Chunk, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockStorer) has(ctx context.Context, hasMode storage.ModeHas, addr boson.Address) (yes bool, err error) {
	switch hasMode {
	case storage.ModeHasChunk:
		_, yes = m.store[addr.String()]
	case storage.ModeHasPin:
		for _, pinAddr := range m.pinnedAddress {
			if pinAddr.Equal(addr) {
				yes = true
				break
			}
		}
	}
	return
}

func (m *MockStorer) Has(ctx context.Context, hasMode storage.ModeHas, addr boson.Address) (yes bool, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.has(ctx, hasMode, addr)
}

func (m *MockStorer) HasMulti(ctx context.Context, hasMode storage.ModeHas, addrs ...boson.Address) (yes []bool, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockStorer) Set(ctx context.Context, mode storage.ModeSet, addrs ...boson.Address) (err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, addr := range addrs {
		m.modeSet[addr.String()] = mode
		switch mode {
		case storage.ModeSetPin:
			// check if chunk exists
			has, err := m.has(ctx, storage.ModeHasChunk, addr)
			if err != nil {
				return err
			}

			if !has {
				return storage.ErrNotFound
			}

			// if mode is set pin, increment the pin counter
			var found bool
			for i, ad := range m.pinnedAddress {
				if addr.String() == ad.String() {
					m.pinnedCounter[i] = m.pinnedCounter[i] + 1
					found = true
				}
			}
			if !found {
				m.pinnedAddress = append(m.pinnedAddress, addr)
				m.pinnedCounter = append(m.pinnedCounter, uint64(1))
			}
		case storage.ModeSetUnpin:
			// if mode is set unpin, decrement the pin counter and remove the address
			// once it reaches zero
			for i, ad := range m.pinnedAddress {
				if addr.String() == ad.String() {
					m.pinnedCounter[i] = m.pinnedCounter[i] - 1
					if m.pinnedCounter[i] == 0 {
						copy(m.pinnedAddress[i:], m.pinnedAddress[i+1:])
						m.pinnedAddress[len(m.pinnedAddress)-1] = boson.NewAddress([]byte{0})
						m.pinnedAddress = m.pinnedAddress[:len(m.pinnedAddress)-1]

						copy(m.pinnedCounter[i:], m.pinnedCounter[i+1:])
						m.pinnedCounter[len(m.pinnedCounter)-1] = uint64(0)
						m.pinnedCounter = m.pinnedCounter[:len(m.pinnedCounter)-1]
					}
				}
			}
		case storage.ModeSetRemove:
			delete(m.store, addr.String())
		default:
		}
	}
	return nil
}
func (m *MockStorer) GetModePut(addr boson.Address) (mode storage.ModePut) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mode, ok := m.modePut[addr.String()]; ok {
		return mode
	}
	return mode
}

func (m *MockStorer) GetModeSet(addr boson.Address) (mode storage.ModeSet) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mode, ok := m.modeSet[addr.String()]; ok {
		return mode
	}
	return mode
}

func (m *MockStorer) Close() error {
	close(m.quit)
	return nil
}

type Option interface {
	apply(*MockStorer)
}

//type optionFunc func(*MockStorer)
//
//func (f optionFunc) apply(r *MockStorer) { f(r) }
