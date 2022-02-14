package wiredtiger

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type batch struct {
	s  *session
	db *DB
}

func (b *batch) Put(key driver.Key, value driver.Value) (err error) {
	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	c, err := b.s.openCursor(obj, &cursorOption{Overwrite: true})
	if err != nil {
		return err
	}

	defer b.s.closeCursor(c)

	return c.insert(k, value.Data)
}

func (b *batch) Delete(key driver.Key) (err error) {
	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	if len(k) == 0 {
		return nil
	}

	c, err := b.s.openCursor(obj, nil)
	if err != nil {
		return err
	}

	defer b.s.closeCursor(c)

	err = c.remove(k)
	if err != nil && IsNotFound(err) {
		return driver.ErrNotFound
	}

	return
}

func (b *batch) Commit() (err error) {
	b.db.pool.Put(b.s)
	return nil
}

func (db *DB) NewBatch() driver.Batching {
	s := db.pool.Get()

	return &batch{
		db: db,
		s:  s,
	}
}
