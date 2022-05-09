package weight

import (
	"fmt"
	"testing"
)

func TestWeightRandom(t *testing.T) {
	w := []int64{250000, 280000, 200000, 300000, 150000}
	for x := 0; x < 5; x++ {
		weight, _ := New(w)
		i := weight.PickIndex()
		w[i] = 0
		fmt.Printf("weight random: %d \n", i+1)
	}
}
