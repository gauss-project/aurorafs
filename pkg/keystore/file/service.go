package file

import (
	"crypto/ecdsa"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gauss-project/aurorafs/pkg/crypto"
)

// Service is the file-based keystore.Service implementation.
//
// Keys are stored in directory where each private key is stored in a file,
// which is encrypted with symmetric key using some password.
type Service struct {
	dir string
}

// New creates new file-based keystore.Service implementation.
func New(dir string) *Service {
	return &Service{dir: dir}
}

func (s *Service) Exists(name string) (bool, error) {
	filename := s.keyFilename(name)

	data, err := os.ReadFile(filename)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read private key: %w", err)
	}
	if len(data) == 0 {
		return false, nil
	}

	return true, nil
}

func (s *Service) Key(name, password string) (pk *ecdsa.PrivateKey, created bool, err error) {
	filename := s.keyFilename(name)

	data, err := os.ReadFile(filename)
	if err != nil && !os.IsNotExist(err) {
		return nil, false, fmt.Errorf("read private key: %w", err)
	}
	if len(data) == 0 {
		var err error
		pk, err = crypto.GenerateSecp256k1Key()
		if err != nil {
			return nil, false, fmt.Errorf("generate secp256k1 key: %w", err)
		}

		d, err := encryptKey(pk, password)
		if err != nil {
			return nil, false, err
		}

		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			return nil, false, err
		}
		if err := os.WriteFile(filename, d, 0600); err != nil {
			return nil, false, err
		}
		return pk, true, nil
	}

	pk, err = decryptKey(data, password)
	if err != nil {
		return nil, false, err
	}
	return pk, false, nil
}

func (s *Service) keyFilename(name string) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s.key", name))
}

func (s *Service) BackKey(name string) error {
	filename := s.keyFilename(name)
	return os.Rename(filename, filename+fmt.Sprintf(".bak.%d", time.Now().Unix()))
}

func (s *Service) ImportKey(name, password string, pk *ecdsa.PrivateKey) error {
	filename := s.keyFilename(name)

	d, err := encryptKey(pk, password)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filename, d, 0600); err != nil {
		return err
	}
	return nil
}
