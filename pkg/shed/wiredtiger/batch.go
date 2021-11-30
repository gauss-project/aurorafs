package wiredtiger

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type batch struct {
	*transaction
}

func (b *batch) Put(key driver.Key, value driver.Value) (err error) {
	return b.transaction.Put(key, value)
}

func (b *batch) Delete(key driver.Key) (err error) {
	return b.transaction.Delete(key)
}

func (b *batch) Commit() (err error) {
	err = b.transaction.Commit()
	if err != nil {
		_ = b.transaction.Rollback()
	}
	return
}

func (db *DB) NewBatch() driver.Batching {
	// disable sync
	return &batch{
		transaction: db.newTxn(false),
	}
}
