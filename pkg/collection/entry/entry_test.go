package entry_test

import (
	"testing"

	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
)

// TestEntrySerialize verifies integrity of serialization.
func TestEntrySerialize(t *testing.T) {
	referenceAddress := test.RandomAddress()
	metadataAddress := test.RandomAddress()
	e := entry.New(referenceAddress, metadataAddress)
	entrySerialized, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	entryRecovered := &entry.Entry{}
	err = entryRecovered.UnmarshalBinary(entrySerialized)
	if err != nil {
		t.Fatal(err)
	}

	if !referenceAddress.Equal(entryRecovered.Reference()) {
		t.Fatalf("expected reference %s, got %s", referenceAddress, entryRecovered.Reference())
	}

	metadataAddressRecovered := entryRecovered.Metadata()
	if !metadataAddress.Equal(metadataAddressRecovered) {
		t.Fatalf("expected metadata %s, got %s", metadataAddress, metadataAddressRecovered)
	}
}
