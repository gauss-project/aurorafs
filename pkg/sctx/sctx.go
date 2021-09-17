// Package sctx provides convenience methods for context
// value injection and extraction.
package sctx

import (
	"context"
	"errors"
	"math/big"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

var (
	// ErrTargetPrefix is returned when target prefix decoding fails.
	ErrTargetPrefix = errors.New("error decoding prefix string")
)

type (
	HTTPRequestIDKey  struct{}
	requestHostKey    struct{}
	tagKey            struct{}
	targetsContextKey struct{}
	gasPriceKey       struct{}
	gasLimitKey       struct{}
	rootCIDKey        struct{}
	localGetKey       struct{}
)

// SetHost sets the http request host in the context
func SetHost(ctx context.Context, domain string) context.Context {
	return context.WithValue(ctx, requestHostKey{}, domain)
}

// GetHost gets the request host from the context
func GetHost(ctx context.Context) string {
	v, ok := ctx.Value(requestHostKey{}).(string)
	if ok {
		return v
	}
	return ""
}

func SetGasLimit(ctx context.Context, limit uint64) context.Context {
	return context.WithValue(ctx, gasLimitKey{}, limit)
}

func GetGasLimit(ctx context.Context) uint64 {
	v, ok := ctx.Value(gasLimitKey{}).(uint64)
	if ok {
		return v
	}
	return 0
}

func SetGasPrice(ctx context.Context, price *big.Int) context.Context {
	return context.WithValue(ctx, gasPriceKey{}, price)
}

func GetGasPrice(ctx context.Context) *big.Int {
	v, ok := ctx.Value(gasPriceKey{}).(*big.Int)
	if ok {
		return v
	}
	return nil
}

func SetRootCID(ctx context.Context, rootCID boson.Address) context.Context {
	return context.WithValue(ctx, rootCIDKey{}, rootCID)
}

func GetRootCID(ctx context.Context) boson.Address {
	v, ok := ctx.Value(rootCIDKey{}).(boson.Address)
	if ok {
		return v
	}
	return boson.ZeroAddress
}

func SetLocalGet(ctx context.Context) context.Context {
	return context.WithValue(ctx, localGetKey{}, true)
}

func GetLocalGet(ctx context.Context) bool {
	_, ok := ctx.Value(localGetKey{}).(bool)
	return ok
}
