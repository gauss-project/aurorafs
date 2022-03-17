package wiredtiger

import (
	"errors"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type snapshot struct {
	s      *session
	mu     sync.RWMutex
	closed bool
}

var ErrSnapshotReleased = errors.New("wiredtiger: snapshot released")

func (s *snapshot) Get(key driver.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrSnapshotReleased
	}
	obj, k := parseKey(key)
	c, err := s.s.openCursor(obj, &cursorOption{
		Raw:      true,
		ReadOnce: true,
	})
	if err != nil {
		return nil, err
	}
	defer s.s.closeCursor(c)
	r, err := c.find(k)
	if err != nil {
		if IsNotFound(err) {
			return nil, driver.ErrNotFound
		}
		return nil, err
	}
	defer r.Close()
	return r.Value(), nil
}

func (s *snapshot) Has(key driver.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrSnapshotReleased
	}
	obj, k := parseKey(key)
	c, err := s.s.openCursor(obj, &cursorOption{
		Raw:      true,
		ReadOnce: true,
	})
	if err != nil {
		return false, err
	}
	defer s.s.closeCursor(c)
	r, err := c.find(k)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	_ = r.Close()
	return true, nil
}

func (s *snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.s.close()
}
