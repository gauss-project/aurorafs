package wiredtiger

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type batch struct {
	db *DB
}

func (b *batch) Put(key driver.Key, value driver.Value) (err error) {
	return b.db.Put(key, value)
}

func (b *batch) Delete(key driver.Key) (err error) {
	defer func() {
		if IsNotFound(err) {
			err = driver.ErrNotFound
		}
	}()
	return b.db.Delete(key)
}

func (b *batch) Commit() (err error) {
	return nil
}

func (db *DB) NewBatch() driver.Batching {
	b := &batch{db: db}

	return b
}
