//go:generate sh -c "protoc -I . -I \"$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)/protobuf\" --gogofaster_out=. multicast.proto"

// Package pb holds only Protocol Buffer definitions and generated code.
package pb
