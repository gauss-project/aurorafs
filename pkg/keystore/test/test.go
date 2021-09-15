package test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/keystore"
)

// Service is a utility testing function that can be used to test
// implementations of the keystore.Service interface.
func Service(t *testing.T, s keystore.Service) {
	exists, err := s.Exists("boson")
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("should not exist")
	}
	// create a new boson key
	k1, created, err := s.Key("boson", "pass123456")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("key is not created")
	}

	exists, err = s.Exists("boson")
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("should exist")
	}

	// get boson key
	k2, created, err := s.Key("boson", "pass123456")
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("key is created, but should not be")
	}
	if !bytes.Equal(k1.D.Bytes(), k2.D.Bytes()) {
		t.Fatal("two keys are not equal")
	}

	// invalid password
	_, _, err = s.Key("boson", "invalid password")
	if !errors.Is(err, keystore.ErrInvalidPassword) {
		t.Fatal(err)
	}

	// create a new libp2p key
	k3, created, err := s.Key("libp2p", "p2p pass")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("key is not created")
	}
	if bytes.Equal(k1.D.Bytes(), k3.D.Bytes()) {
		t.Fatal("two keys are equal, but should not be")
	}

	// get libp2p key
	k4, created, err := s.Key("libp2p", "p2p pass")
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("key is created, but should not be")
	}
	if !bytes.Equal(k3.D.Bytes(), k4.D.Bytes()) {
		t.Fatal("two keys are not equal")
	}
}
