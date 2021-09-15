package ratelimit_test

import (
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/ratelimit"
)

func TestRateLimit(t *testing.T) {

	var (
		key1  = "test1"
		key2  = "test2"
		rate  = time.Second
		burst = 10
	)

	limiter := ratelimit.New(rate, burst)

	if !limiter.Allow(key1, burst) {
		t.Fatal("want allowed")
	}

	if limiter.Allow(key1, burst) {
		t.Fatalf("want not allowed")
	}

	limiter.Clear(key1)

	if !limiter.Allow(key1, burst) {
		t.Fatal("want allowed")
	}

	if !limiter.Allow(key2, burst) {
		t.Fatal("want allowed")
	}
}
