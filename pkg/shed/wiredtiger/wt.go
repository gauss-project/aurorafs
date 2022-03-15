package wiredtiger

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lwiredtiger

#include <stdlib.h>
#include <wiredtiger.h>

int wiredtiger_connection_close(WT_CONNECTION *connection, const char *config) {
	return connection->close(connection, config);
}
*/
import "C"
import (
	"fmt"
	"syscall"
	"time"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

func init() {
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}
}

type DB struct {
	config  Configuration
	conn    *C.WT_CONNECTION
	pool    *sessionPool
	closing chan struct{}
}

func checkError(result int) bool {
	return result != 0 && result != int(syscall.ENOTSUP)
}

func (db *DB) Close() error {
	quit := make(chan struct{})

	go func() {
		s := db.pool.Get()
		defer db.pool.Put(s)
		if s != nil {
			err := s.checkpoint()
			if err != nil {
				logger.Errorf("wiredtiger: create checkpoint: %v", err)
			}
		}

		err := db.pool.Close()
		if err != nil {
			logger.Errorf("wiredtiger: close session pool: %v", err)
		}

		close(quit)
	}()

	select {
	case <-quit:
	case <-time.After(5 * time.Second):
		// TODO timeout handle
	}

	// If the process is exiting regardless, configuring WT_CONNECTION::close to leak memory on close can significantly speed up the close.
	result := int(C.wiredtiger_connection_close(db.conn, nil))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func parseKey(key driver.Key) (dataSource, []byte) {
	obj := dataSource{dataType: tableSource}

	if key.Prefix == 0 {
		obj.sourceName = stateTableName

		return obj, key.Data
	}

	switch key.Data[0] {
	case fieldKeyPrefix:
		obj.sourceName = fieldTableName
	default:
		obj.sourceName = indexTablePrefix + string(key.Data[:key.Prefix])
	}

	return obj, key.Data[key.Prefix:]
}

func (db *DB) Get(key driver.Key) ([]byte, error) {
	s := db.pool.Get()
	if s == nil {
		return nil, ErrSessionHasClosed
	}

	defer db.pool.Put(s)

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return nil, ErrInvalidArgument
	}

	c, err := s.openCursor(obj, nil)
	if err != nil {
		return nil, err
	}

	result, err := c.find(k)
	if err != nil {
		if IsNotFound(err) {
			return nil, driver.ErrNotFound
		}
		return nil, err
	}

	defer result.Close()

	return result.Value(), nil
}

func (db *DB) Has(key driver.Key) (bool, error) {
	s := db.pool.Get()
	if s == nil {
		return false, ErrSessionHasClosed
	}

	defer db.pool.Put(s)

	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return false, ErrInvalidArgument
	}

	c, err := s.openCursor(obj, nil)
	if err != nil {
		return false, err
	}

	result, err := c.find(k)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	_ = result.Close()

	return true, nil
}

func (db *DB) Put(key driver.Key, value driver.Value) error {
	s := db.pool.Get()
	if s == nil {
		return ErrSessionHasClosed
	}

	defer db.pool.Put(s)

	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	c, err := s.openCursor(obj, &cursorOption{Overwrite: true})
	if err != nil {
		return err
	}

	return c.insert(k, value.Data)
}

func (db *DB) Delete(key driver.Key) error {
	s := db.pool.Get()
	if s == nil {
		return ErrSessionHasClosed
	}

	defer db.pool.Put(s)

	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	c, err := s.openCursor(obj, nil)
	if err != nil {
		return err
	}

	return c.remove(k)
}

func (db *DB) Search(query driver.Query) (cursor driver.Cursor) {
	s := db.pool.Get()
	if s == nil {
		return &errorCursor{
			err: ErrSessionHasClosed,
		}
	}

	defer func() {
		if _, ok := cursor.(*errorCursor); ok {
			db.pool.Put(s)
		}
	}()

	var obj dataSource

	// parse source
	obj, k := parseKey(query.Prefix)
	if obj.dataType == "" {
		return &errorCursor{
			err: fmt.Errorf("wiredtiger: parse unknown key type: %s", k),
		}
	}

	c, err := s.openCursor(obj, nil)
	if err != nil {
		return &errorCursor{
			err: fmt.Errorf("wiredtiger: open cursor for table %s: %v", obj.sourceName, err),
		}
	}

	sc, err := c.search(k)
	if err != nil {
		if IsNotFound(err) {
			return InvalidCursor
		}

		return &errorCursor{
			err: fmt.Errorf("wiredtiger: search key %s in table %s: %v", k, obj.sourceName, err),
		}
	}

	sc.prefix = query.Prefix.Data[:query.Prefix.Prefix]

	return sc
}

func (db *DB) GetSnapshot() (driver.Snapshot, error) {
	s := db.pool.Get()
	if s == nil {
		return nil, ErrSessionHasClosed
	}

	return &snapshot{s: s}, nil
}
