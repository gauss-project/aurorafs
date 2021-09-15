package entry

import (
	"errors"
	"math"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/collection"
	"github.com/gauss-project/aurorafs/pkg/encryption"
)

var (
	_                           = collection.Entry(&Entry{})
	serializedDataSize          = boson.SectionSize * 2
	encryptedSerializedDataSize = encryption.ReferenceSize * 2
)

// Entry provides addition of metadata to a data reference.
// Implements collection.Entry.
type Entry struct {
	reference boson.Address
	metadata  boson.Address
}

// New creates a new Entry.
func New(reference, metadata boson.Address) *Entry {
	return &Entry{
		reference: reference,
		metadata:  metadata,
	}
}

// CanUnmarshal returns whether the entry may be might be unmarshaled based on
// the size.
func CanUnmarshal(size int64) bool {
	if size < math.MaxInt32 {
		switch int(size) {
		case serializedDataSize, encryptedSerializedDataSize:
			return true
		}
	}
	return false
}

// Reference implements collection.Entry
func (e *Entry) Reference() boson.Address {
	return e.reference
}

// Metadata implements collection.Entry
func (e *Entry) Metadata() boson.Address {
	return e.metadata
}

// MarshalBinary implements encoding.BinaryMarshaler
func (e *Entry) MarshalBinary() ([]byte, error) {
	br := e.reference.Bytes()
	bm := e.metadata.Bytes()
	b := append(br, bm...)
	return b, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (e *Entry) UnmarshalBinary(b []byte) error {
	var size int
	if len(b) == serializedDataSize {
		size = serializedDataSize
	} else if len(b) == encryptedSerializedDataSize {
		size = encryptedSerializedDataSize
	} else {
		return errors.New("invalid data length")
	}
	e.reference = boson.NewAddress(b[:size/2])
	e.metadata = boson.NewAddress(b[size/2:])
	return nil
}
