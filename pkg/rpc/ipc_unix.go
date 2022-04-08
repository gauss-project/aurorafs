// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package rpc

import (
	"context"
	"net"
	"os"
	"path/filepath"

	"github.com/gauss-project/aurorafs/pkg/logging"
)

// ipcListen will create a Unix socket on the given endpoint.
func ipcListen(endpoint string) (net.Listener, error) {
	if len(endpoint) > int(max_path_size) {
		logging.Warningf("The ipc endpoint is longer than %d characters. endpoint %s", max_path_size, endpoint)
	}

	// Ensure the IPC path exists and remove any previous leftover
	if err := os.MkdirAll(filepath.Dir(endpoint), 0751); err != nil {
		return nil, err
	}
	_ = os.Remove(endpoint)
	l, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, err
	}
	return l, os.Chmod(endpoint, 0600)
}

// newIPCConnection will connect to a Unix socket on the given endpoint.
func newIPCConnection(ctx context.Context, endpoint string) (net.Conn, error) {
	return new(net.Dialer).DialContext(ctx, "unix", endpoint)
}
