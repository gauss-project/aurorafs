package mockbytes

import (
	"io"
	"math/rand"
)

const (
	MockTypeStandard = iota
)

type MockBytes struct {
	io.Reader
	seed int
	mod  int
}

func New(i int, typ int) *MockBytes {
	var s rand.Source
	switch {
	default:
		s = rand.NewSource(int64(i))
	}
	return &MockBytes{
		Reader: rand.New(s),
		seed:   i,
	}
}

func (m *MockBytes) WithModulus(mod int) *MockBytes {
	m.mod = mod
	return m
}

func (m *MockBytes) RandomBytes(c int) ([]byte, error) {
	b := make([]byte, c)
	for i := 0; i < c; {
		r, err := m.Read(b)
		if err != nil {
			return nil, err
		}
		i += r
	}
	return b, nil
}

func (m *MockBytes) SequentialBytes(c int) ([]byte, error) {
	b := make([]byte, c)
	v := m.seed
	if m.mod > 0 {
		for i := 0; i < c; i++ {
			b[i] = byte(v % m.mod)
			v++
		}
	} else {
		for i := 0; i < c; i++ {
			b[i] = byte(v)
			v++
		}
	}
	return b, nil
}
