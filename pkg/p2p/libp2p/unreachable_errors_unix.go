//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package libp2p

import "golang.org/x/sys/unix"

// Collection of errors returned by the underlying
// operating system that signals network unavailability.
var (
	errHostUnreachable    error = unix.EHOSTUNREACH
	errNetworkUnreachable error = unix.ENETUNREACH
)
