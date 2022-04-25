//go:build windows
// +build windows

package libp2p

import "golang.org/x/sys/windows"

// Collection of errors returned by the underlying
// operating system that signals network unavailability.
var (
	errHostUnreachable    error = windows.WSAEHOSTUNREACH
	errNetworkUnreachable error = windows.WSAENETUNREACH
)
