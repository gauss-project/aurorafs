// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package shed provides a simple abstraction components to compose
// more complex operations on storage data organized in fields and indexes.
//
// Only type which holds logical information about boson storage chunks data
// and metadata is Item. This part is not generalized mostly for
// performance reasons.
package shed

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]driver.Driver)
)

type ErrDriverNotRegister struct {
	Name string
}

func (e ErrDriverNotRegister) Error() string {
	return fmt.Sprintf("driver %s not register\n", e.Name)
}

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver driver.Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	list := make([]string, 0, len(drivers))
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// DB provides abstractions over database driver in order to
// implement complex structures using fields and ordered indexes.
// It provides a schema functionality to store fields and indexes
// information about naming and types.
type DB struct {
	backend driver.BatchDB
	metrics metrics
	quit    chan struct{} // Quit channel to stop the metrics collection before closing the database
}

type Options struct {
	Driver string
}

// NewDB constructs a new DB and validates the schema
// if it exists in database on the given path.
// metricsPrefix is used for metrics collection for the given DB.
func NewDB(path string, o *Options) (db *DB, err error) {
	var (
		drv    string
		config string
	)

	if o != nil {
		idx := strings.IndexByte(o.Driver, ':')
		if idx == -1 {
			drv = o.Driver
		} else {
			drv = o.Driver[:idx]
			config = o.Driver[idx+1:]
		}
	}

	if drv == "" {
		if len(Drivers()) == 0 {
			return nil, fmt.Errorf("no available database driver")
		}
		drv = Drivers()[0]
	}

	d, ok := drivers[drv]
	if !ok {
		return nil, ErrDriverNotRegister{Name: drv}
	}

	i, err := d.Open(path, config)

	if err != nil {
		return nil, err
	}

	bi, ok := i.(driver.BatchDB)
	if !ok {
		return nil, fmt.Errorf("current backend %s not support batching", drv)
	}

	return NewDBWrap(bi)
}

// NewDBWrap returns new DB which uses the given database as its underlying storage.
// The function will panics if the given database is nil.
func NewDBWrap(i driver.BatchDB) (db *DB, err error) {
	if i == nil {
		panic(errors.New("shed: NewDBWrap: nil db backend"))
	}

	db = &DB{
		backend: i,
		metrics: newMetrics(),
	}

	err = db.initSchema()
	if err != nil {
		return nil, err
	}

	// Create a quit channel for the periodic metrics collector and run it.
	db.quit = make(chan struct{})

	return db, nil
}

// Put wraps database Put method to increment metrics counter.
func (db *DB) Put(prefix int, key, value []byte) (err error) {
	err = db.backend.Put(driver.Key{Prefix: prefix, Data: key}, driver.Value{Data: value})
	if err != nil {
		db.metrics.PutFailCounter.Inc()
		return err
	}
	db.metrics.PutCounter.Inc()
	return nil
}

// Get wraps database Get method to increment metrics counter.
func (db *DB) Get(prefix int, key []byte) (value []byte, err error) {
	value, err = db.backend.Get(driver.Key{Prefix: prefix, Data: key})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			db.metrics.GetNotFoundCounter.Inc()
		} else {
			db.metrics.GetFailCounter.Inc()
		}
		return nil, err
	}
	db.metrics.GetCounter.Inc()
	return value, nil
}

// Has wraps database Has method to increment metrics counter.
func (db *DB) Has(prefix int, key []byte) (yes bool, err error) {
	yes, err = db.backend.Has(driver.Key{Prefix: prefix, Data: key})
	if err != nil {
		db.metrics.HasFailCounter.Inc()
		return false, err
	}
	db.metrics.HasCounter.Inc()
	return yes, nil
}

// Delete wraps database Delete method to increment metrics counter.
func (db *DB) Delete(prefix int, key []byte) (err error) {
	err = db.backend.Delete(driver.Key{Prefix: prefix, Data: key})
	if err != nil {
		db.metrics.DeleteFailCounter.Inc()
		return err
	}
	db.metrics.DeleteCounter.Inc()
	return nil
}

type Batch struct {
	db *DB
	driver.Batching
}

// Commit wraps database Commit method to increment metrics counter.
func (b *Batch) Commit() (err error) {
	err = b.Batching.Commit()
	if err != nil {
		b.db.metrics.WriteBatchFailCounter.Inc()
		return err
	}
	b.db.metrics.WriteBatchCounter.Inc()
	return nil
}

func (db *DB) NewBatch() driver.Batching {
	return &Batch{
		Batching: db.backend.NewBatch(),
		db:       db,
	}
}

// Close closes database backend.
func (db *DB) Close() (err error) {
	close(db.quit)
	return db.backend.Close()
}
