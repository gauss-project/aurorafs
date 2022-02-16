package leveldb

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/gauss-project/aurorafs/pkg/shed/leveldb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	ldberr "github.com/syndtr/goleveldb/leveldb/errors"
)

var _ storage.StateStorer = (*store)(nil)

var leveldbDriver = leveldb.Driver{}

// store uses LevelDB to store values.
type store struct {
	db     driver.BatchDB
	logger logging.Logger
}

func NewInMemoryStateStore(l logging.Logger) (storage.StateStorer, error) {
	ldb, err := leveldbDriver.Open("")
	if err != nil {
		return nil, err
	}

	s := &store{
		db:     ldb.(driver.BatchDB),
		logger: l,
	}

	if err := migrate(s); err != nil {
		return nil, err
	}

	return s, nil
}

// NewStateStore creates a new persistent state storage.
func NewStateStore(path string, l logging.Logger) (storage.StateStorer, error) {
	db, err := leveldbDriver.Open(path)
	if err != nil {
		if !ldberr.IsCorrupted(err) {
			return nil, err
		}

		// l.Warningf("statestore open failed: %v. attempting recovery", err)
		// db, err = leveldb.RecoverFile(path, nil)
		// if err != nil {
		// 	return nil, fmt.Errorf("statestore recovery: %w", err)
		// }
		// l.Warning("statestore recovery ok! you are kindly request to inform us about the steps that preceded the last aurora shutdown.")
	}

	s := &store{
		db:     db.(driver.BatchDB),
		logger: l,
	}

	if err := migrate(s); err != nil {
		return nil, err
	}

	return s, nil
}

func migrate(s *store) error {
	sn, err := s.getSchemaName()
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			_ = s.Close()
			return fmt.Errorf("get schema name: %w", err)
		}
		// new statestore - put schema key with current name
		if err := s.putSchemaName(dbSchemaCurrent); err != nil {
			_ = s.Close()
			return fmt.Errorf("put schema name: %w", err)
		}
		sn = dbSchemaCurrent
	}

	if err = s.migrate(sn); err != nil {
		_ = s.Close()
		return fmt.Errorf("migrate: %w", err)
	}

	return nil
}

// Get retrieves a value of the requested key. If no results are found,
// storage.ErrNotFound will be returned.
func (s *store) Get(key string, i interface{}) error {
	data, err := s.db.Get(driver.Key{Data: []byte(key)})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}

	if unmarshaler, ok := i.(encoding.BinaryUnmarshaler); ok {
		return unmarshaler.UnmarshalBinary(data)
	}

	return json.Unmarshal(data, i)
}

// Put stores a value for an arbitrary key. BinaryMarshaler
// interface method will be called on the provided value
// with fallback to JSON serialization.
func (s *store) Put(key string, i interface{}) (err error) {
	var bytes []byte
	if marshaler, ok := i.(encoding.BinaryMarshaler); ok {
		if bytes, err = marshaler.MarshalBinary(); err != nil {
			return err
		}
	} else if bytes, err = json.Marshal(i); err != nil {
		return err
	}

	return s.db.Put(driver.Key{Data: []byte(key)}, driver.Value{Data: bytes})
}

// Delete removes entries stored under a specific key.
func (s *store) Delete(key string) (err error) {
	return s.db.Delete(driver.Key{Data: []byte(key)})
}

// Iterate entries that match the supplied prefix.
func (s *store) Iterate(prefix string, iterFunc storage.StateIterFunc) (err error) {
	iter := s.db.Search(driver.Query{Prefix: driver.Key{Data: []byte(prefix)}, MatchPrefix: true})
	defer func() {
		err = iter.Close()
	}()
	for ; iter.Valid(); iter.Next() {
		stop, err := iterFunc(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return iter.Error()
}

func (s *store) getSchemaName() (string, error) {
	name, err := s.db.Get(driver.Key{Data: []byte(dbSchemaKey)})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return "", storage.ErrNotFound
		}
		return "", err
	}
	return string(name), nil
}

func (s *store) putSchemaName(val string) error {
	return s.db.Put(driver.Key{Data: []byte(dbSchemaKey)}, driver.Value{Data: []byte(val)})
}

// DB implements StateStorer.DB method.
func (s *store) DB() driver.BatchDB {
	return s.db
}

// Close releases the resources used by the store.
func (s *store) Close() error {
	return s.db.Close()
}
