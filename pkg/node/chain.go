// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"github.com/ethersphere/bee/pkg/oracle"
	"time"

	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/storage"
)

const (
	maxDelay          = 1 * time.Minute
	cancellationDepth = 6
)

// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transaction Service to interact with it using the provided signer.
func InitChain(
	ctx context.Context,
	logger logging.Logger,
	stateStore storage.StateStorer,
	endpoint string,
	signer crypto.Signer,
	pollingInterval time.Duration,
) (oracle.ChainOracle, error) {
	return oracle.Init(logger,endpoint,pollingInterval)
}
