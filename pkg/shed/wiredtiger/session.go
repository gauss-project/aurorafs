package wiredtiger

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lwiredtiger

#include <stdlib.h>
#include <wiredtiger.h>

int wiredtiger_open_session(WT_CONNECTION *connection, WT_EVENT_HANDLER *errhandler, const char *config, WT_SESSION **sessionp) {
	return connection->open_session(connection, errhandler, config, sessionp);
}

int wiredtiger_session_create(WT_SESSION *session, const char *name, const char *config) {
	return session->create(session, name, config);
}

int wiredtiger_session_compact(WT_SESSION *session, const char *name, const char *config) {
	return session->compact(session, name, config);
}

int wiredtiger_session_drop(WT_SESSION *session, const char *name, const char *config) {
	return session->drop(session, name, config);
}

int wiredtiger_session_checkpoint(WT_SESSION *session, const char *config) {
	return session->checkpoint(session, config);
}

const char *wiredtiger_session_strerror(WT_SESSION *session, int error) {
	return session->strerror(session, error);
}

int wiredtiger_session_reset(WT_SESSION *session) {
	return session->reset(session);
}

int wiredtiger_session_close(WT_SESSION *session, const char *config) {
	return session->close(session, config);
}
*/
import "C"
import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var sessionMaxSize uint64

type sessionPool struct {
	conn    *C.WT_CONNECTION
	session *session
	lock    sync.Mutex
	wakeup  chan struct{}
	id      uint64
	closed  bool
}

func newSessionPool(conn *C.WT_CONNECTION) (*sessionPool, error) {
	p := &sessionPool{
		conn:   conn,
		wakeup: make(chan struct{}),
	}

	s, err := newSession(p.conn, p.getSessionID())
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	s.pooled(p)
	p.lock.Unlock()

	select {
	case p.wakeup <- struct{}{}:
	default:
	}

	return p, nil
}

func (p *sessionPool) getSessionID() uint64 {
	id := atomic.AddUint64(&p.id, 1)
	if id == math.MaxUint64 {
		if !atomic.CompareAndSwapUint64(&p.id, math.MaxUint64-1, 1) {
			id = atomic.AddUint64(&p.id, 1)
		}
	}
	return id
}

var ErrSessionHasClosed = errors.New("session has closed")

func (p *sessionPool) Get() *session {
	err := setTimestamp(p.conn, fmt.Sprintf("%d", time.Now().UnixMilli()), true)
	if err != nil {
		logger.Warnf("set stable timestamp: %v", err)
	}

	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	p.lock.Lock()

	for p.session == nil {
		if p.closed {
			p.lock.Unlock()
			return nil
		}

		p.lock.Unlock()
		<-t.C
		p.lock.Lock()
	}

	defer func() {
		p.session = nil
		p.lock.Unlock()
	}()

	return p.session
}

func (p *sessionPool) Put(s *session) {
	p.lock.Lock()
	p.session = s
	p.lock.Unlock()
}

func (p *sessionPool) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// set flag to closed
	p.closed = true
	close(p.wakeup)

	if p.session == nil {
		return nil
	}

	return p.session.close()
}

type session struct {
	ref     *sessionPool
	impl    *C.WT_SESSION
	id      uint64
	closing int32
	cursors *cursorCache
}

func newSession(conn *C.WT_CONNECTION, id uint64) (*session, error) {
	var sessionImpl *C.WT_SESSION

	configStr := C.CString("isolation=snapshot")
	defer C.free(unsafe.Pointer(configStr))

	result := int(C.wiredtiger_open_session(conn, nil, configStr, &sessionImpl))
	if checkError(result) {
		return nil, NewError(result)
	}

	s := &session{
		impl: sessionImpl,
		id:   id,
	}

	s.cursors = newCursorCache()

	return s, nil
}

func (s *session) pooled(p *sessionPool) {
	p.session = s
	s.ref = p
}

func (s *session) strerror(code int) error {
	err := C.wiredtiger_session_strerror(s.impl, C.int(code))
	if err != nil {
		return errors.New(C.GoString(err))
	}
	return nil
}

type dataSource struct {
	dataType   dataType
	sourceName string
	projection string
}

type dataType string

const (
	tableSource dataType = "table"
	indexSource dataType = "index"
)

func (ds dataSource) String() string {
	s := string(ds.dataType)

	if len(ds.sourceName) != 0 {
		s += ":" + ds.sourceName
	}

	if len(ds.projection) != 0 {
		s += ":" + ds.projection
	}

	return s
}

type createOption struct {
	SourceType        string `key:"type"`
	AccessPatternHint string
	AllocationSize    int
	BlockCompressor   Compressor
	KeyFormat         string
	ValueFormat       string
	LeafKeyMax        int
	LeafPageMax       int
	LeafValueMax      int
	MemoryPageMax     int
	PrefixCompression bool
	SplitPct          int
	Checksum          string
}

func (s *session) create(obj dataSource, opt *createOption) error {
	if atomic.LoadInt32(&s.closing) == 1 {
		return ErrSessionClosed
	}

	objStr := C.CString(obj.String())
	optStr := C.CString(structToList(*opt, false))
	defer func() {
		C.free(unsafe.Pointer(objStr))
		C.free(unsafe.Pointer(optStr))
	}()

	result := int(C.wiredtiger_session_create(s.impl, objStr, optStr))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func (s *session) compact(obj dataSource) error {
	if atomic.LoadInt32(&s.closing) == 1 {
		return ErrSessionClosed
	}

	objStr := C.CString(obj.String())
	defer C.free(unsafe.Pointer(objStr))

	// default disable compact timeout
	configStr := C.CString("timeout=0")
	defer C.free(unsafe.Pointer(configStr))

	result := int(C.wiredtiger_session_compact(s.impl, objStr, configStr))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func (s *session) checkpoint() error {
	var configStr *C.char = nil

	result := int(C.wiredtiger_session_checkpoint(s.impl, configStr))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}

func (s *session) openCursor(obj dataSource, opt *cursorOption) (*cursor, error) {
	if atomic.LoadInt32(&s.closing) == 1 {
		return nil, ErrSessionClosed
	}
	if opt == nil {
		opt = &cursorOption{Raw: true}
	}
	c, err := s.cursors.newCursor(s, obj.String(), opt)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (s *session) closeCursor(c *cursor) error {
	s.cursors.releaseCursor(c)
	return nil
}

func (s *session) reset() error {
	// if session was closing, skip reset
	if atomic.LoadInt32(&s.closing) == 1 {
		// already closing
		return nil
	}
	result := int(C.wiredtiger_session_reset(s.impl))
	if checkError(result) {
		return NewError(result, s)
	}
	return nil
}

func (s *session) close() error {
	if !atomic.CompareAndSwapInt32(&s.closing, 0, 1) {
		// already closing
		return nil
	}
	err := s.cursors.closeAll("")
	if err != nil {
		return err
	}
	result := int(C.wiredtiger_session_close(s.impl, nil))
	if checkError(result) {
		return NewError(result, s)
	}
	return nil
}
