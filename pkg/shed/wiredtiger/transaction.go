package wiredtiger

/*
#cgo LDFLAGS: -lwiredtiger
#include <stdlib.h>
#include <wiredtiger.h>

int wiredtiger_session_begin_transaction(WT_SESSION *session, const char *config) {
	return session->begin_transaction(session, config);
}

int wiredtiger_session_commit_transaction(WT_SESSION *session, const char *config) {
	return session->commit_transaction(session, config);
}

int wiredtiger_session_rollback_transaction(WT_SESSION *session, const char *config) {
	return session->rollback_transaction(session, config);
}
*/
import "C"
import (
	"time"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type txnTimestamp struct {
	writeTime  time.Time
	deleteTime time.Time
	commitTime time.Time
}

type transaction struct {
	s     *session
	db    *DB
	txnId uint64
	sync  bool
	err   error
	stat  txnTimestamp
}

func (t *transaction) Put(key driver.Key, value driver.Value) (err error) {
	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	c, err := t.s.openCursor(obj, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			t.stat.writeTime = time.Now()
		}
	}()

	return c.insert(k, value.Data)
}

func (t *transaction) Delete(key driver.Key) (err error) {
	var obj dataSource

	// parse source
	obj, k := parseKey(key)
	if obj.dataType == "" {
		logger.Warnf("wiredtiger: parse unknown key type: %s", k)
		return ErrInvalidArgument
	}

	c, err := t.s.openCursor(obj, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			t.stat.deleteTime = time.Now()
		}
	}()

	return c.remove(k)
}

func (t *transaction) Commit() (err error) {
	if t.err != nil {
		return t.err
	}

	var config *C.char = nil

	if t.sync {
		config = C.CString("sync=on")
	}

	// if commit successful, put session back
	defer func() {
		if err == nil {
			t.stat.commitTime = time.Now()

			t.db.pool.PutLock(t.s, t.txnId)
		}
	}()

	result := int(C.wiredtiger_session_commit_transaction(t.s.impl, config))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func (t *transaction) Rollback() error {
	if !t.stat.commitTime.IsZero() {
		return nil
	}

	// always put it back
	defer t.db.pool.PutLock(t.s, t.txnId)

	result := int(C.wiredtiger_session_commit_transaction(t.s.impl, nil))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func (db *DB) newTxn(sync bool) *transaction {
	s := db.pool.Get()
	t := time.Now()

	var (
		err    error
		config *C.char = nil
	)

	if sync {
		config = C.CString("sync=true")
	}

	result := int(C.wiredtiger_session_begin_transaction(s.impl, config))
	if checkError(result) {
		err = NewError(result)
	}

	return &transaction{
		s:     s,
		db:    db,
		err:   err,
		sync:  sync,
		txnId: uint64(t.UnixNano()),
	}
}

func (db *DB) NewTransaction() driver.Transaction {
	// force sync
	return db.newTxn(true)
}
