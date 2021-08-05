// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"github.com/ethersphere/bee/pkg/oracle"
	"time"

	"github.com/ethersphere/bee/pkg/logging"
)

const (
	maxDelay          = 1 * time.Minute
	cancellationDepth = 6
)

// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transaction Service to interact with it using the provided signer.
func InitChain(
	p2pCtx context.Context,
	logger logging.Logger,

	endpoint string,

	pollingInterval time.Duration,
	networkID uint64,
) (oracle.ChainOracle, error) {
	return oracle.Init(p2pCtx,logger,endpoint,pollingInterval,networkID)
}
