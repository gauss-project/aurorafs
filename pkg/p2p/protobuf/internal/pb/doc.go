//go:generate sh -c "protoc -I . -I \"$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)/protobuf\" --gogofaster_out=. test.proto"

// Package pb holds only Protocol Buffer definitions and generated code for
// testing purposes.
package pb
