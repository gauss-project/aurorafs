package driver

import "io"

type Driver interface {
	Open(dsn, options string) (DB, error)
}

type DB interface {
	Schema
	Reader
	Writer
	Finder
	GetSnapshot() (Snapshot, error)
	io.Closer
}

type TxnDB interface {
	DB
	NewTransaction() Transaction
}

type BatchDB interface {
	DB
	NewBatch() Batching
}

type Reader interface {
	Get(key Key) ([]byte, error)
	Has(key Key) (bool, error)
}

type Writer interface {
	Put(key Key, value Value) error
	Delete(key Key) error
}

type Finder interface {
	Search(query Query) Cursor
}

type Snapshot interface {
	Reader
	io.Closer
}

type Batching interface {
	Writer
	Commit() error
}

type Transaction interface {
	Writer
	Commit() error
	Rollback() error
}

type Cursor interface {
	Next() bool
	Prev() bool
	Last() bool
	Seek(key Key) bool
	Key() []byte
	Value() []byte
	Valid() bool
	Error() error
	io.Closer
}
