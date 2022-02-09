package wiredtiger

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lwiredtiger

#include <errno.h>
#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"
import (
	"errors"
)

type Error struct {
	code int
	err  error
}

func NewError(code int, s ...*session) Error {
	e := Error{code: code}

	if len(s) > 0 {
		e.err = s[0].strerror(code)
	}

	return e
}

func (e Error) Error() string {
	if e.err != nil {
		return e.err.Error()
	}

	err := C.wiredtiger_strerror(C.int(e.code))

	return C.GoString(err)
}

var (
	ErrInvalidArgument = NewError(C.EINVAL)
	ErrBusy            = NewError(C.EBUSY)

	ErrError           = NewError(C.WT_ERROR)
	ErrPanic           = NewError(C.WT_PANIC)
	ErrRollback        = NewError(C.WT_ROLLBACK)
	ErrDuplicateKey    = NewError(C.WT_DUPLICATE_KEY)
	ErrNotFound        = NewError(C.WT_NOTFOUND)
	ErrRunRecovery     = NewError(C.WT_RUN_RECOVERY)
	ErrCacheFull       = NewError(C.WT_CACHE_FULL)
	ErrPrepareConflict = NewError(C.WT_PREPARE_CONFLICT)
)

var (
	ErrSessionClosed = errors.New("session has closed")
)

func isError(e error, t Error) bool {
	v, ok := e.(Error)
	if !ok {
		return false
	}

	return v.code == t.code
}

func IsPanic(e error) bool {
	return isError(e, ErrPanic)
}

func IsRollback(e error) bool {
	return isError(e, ErrRollback)
}

func IsDuplicateKey(e error) bool {
	return isError(e, ErrDuplicateKey)
}

func IsNotFound(e error) bool {
	return isError(e, ErrNotFound)
}

func IsRunRecovery(e error) bool {
	return isError(e, ErrRunRecovery)
}

func IsCacheFull(e error) bool {
	return isError(e, ErrCacheFull)
}

func IsPrepareConflict(e error) bool {
	return isError(e, ErrPrepareConflict)
}
