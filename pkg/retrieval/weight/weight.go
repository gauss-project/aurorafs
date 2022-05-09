package weight

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type Weight struct {
	pre []int64
}

func New(w []int64) (*Weight, error) {
	p := make([]int64, len(w))
	p[0] = w[0]
	for i := 1; i < len(w); i++ {
		p[i] = p[i-1] + w[i]
	}
	if p[len(p)-1] == 0 {
		return nil, fmt.Errorf("maximum value cannot be zero")
	}
	return &Weight{pre: p}, nil
}

func (w *Weight) PickIndex() int {
	rand.Seed(time.Now().UnixNano())
	r := rand.Int63n(w.pre[len(w.pre)-1]) + 1
	return sort.Search(len(w.pre), func(i int) bool {
		return w.pre[i] >= r
	})
}
