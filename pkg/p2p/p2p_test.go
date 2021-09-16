package p2p_test

import (
	"testing"

	"github.com/gauss-project/aurorafs/pkg/p2p"
)

func TestNewAuroraStreamName(t *testing.T) {
	want := "/boson/hive/1.2.0/peers"
	got := p2p.NewAuroraStreamName("hive", "1.2.0", "peers")

	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
