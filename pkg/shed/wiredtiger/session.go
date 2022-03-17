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
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

var sessionMaxSize uint64

type sessionPool struct {
	sessions map[string]*session
	conn     *C.WT_CONNECTION
	lock     sync.RWMutex
	closed   bool
}

func newSessionPool(conn *C.WT_CONNECTION) (*sessionPool, error) {
	p := &sessionPool{
		conn:     conn,
		sessions: make(map[string]*session),
	}

	return p, nil
}

var ErrSessionHasClosed = errors.New("session has closed")

// func (p *sessionPool) Read(table string) *session {
// 	p.lock.RLock()
//
// 	if p.closed {
// 		p.lock.RUnlock()
// 		return nil
// 	}
//
// 	if s, ok := p.sessions[table]; ok {
// 		if atomic.LoadInt32(&s.closing) == 1 {
// 			return nil
// 		}
// 		if atomic.LoadUint32(&s.inuse) == 0 {
// 			p.lock.RUnlock()
// 			return s
// 		}
// 	}
//
// 	p.lock.RUnlock()
//
// 	s, err := newSession(p.conn)
// 	if err != nil {
// 		logger.Errorf("create session: %v", err)
//
// 		return nil
// 	}
//
// 	// link session pool
// 	s.ref = p
//
// 	return s
// }

func (p *sessionPool) Get(table string) *session {
	// TODO move it
	// err := setTimestamp(p.conn, fmt.Sprintf("%d", time.Now().UnixMilli()), true)
	// if err != nil {
	// 	logger.Warnf("set stable timestamp: %v", err)
	// }

	p.lock.RLock()

	if p.closed {
		p.lock.RUnlock()
		return nil
	}

	if s, ok := p.sessions[table]; ok {
		p.lock.RUnlock()
		for {
			if atomic.LoadInt32(&s.closing) == 1 {
				return nil
			}
			if !atomic.CompareAndSwapUint32(&s.inuse, 0, 1) {
				runtime.Gosched()
				continue
			}
			return s
		}
	}

	p.lock.RUnlock()

	// put new session
	p.lock.Lock()

	// double check
	if s, ok := p.sessions[table]; ok {
		p.lock.Unlock()
		for {
			if atomic.LoadInt32(&s.closing) == 1 {
				return nil
			}
			if !atomic.CompareAndSwapUint32(&s.inuse, 0, 1) {
				runtime.Gosched()
				continue
			}
			return s
		}
	}

	s, err := newSession(p.conn)
	if err != nil {
		logger.Errorf("create session: %v", err)

		return nil
	}

	// link session pool
	s.ref = p

	p.sessions[table] = s
	p.lock.Unlock()

	return s
}

func (p *sessionPool) Put(s *session) {
	for {
	retry:
		if atomic.LoadUint32(&s.inuse) == 0 {
			return
		}
		if !atomic.CompareAndSwapUint32(&s.inuse, 1, 0) {
			if atomic.LoadUint32(&s.inuse) == 1 {
				goto retry
			}

			runtime.Gosched()
			continue
		}

		return
	}
}

// func (p *sessionPool) CloseRead(s *session) {
// 	err := s.close()
// 	if err != nil {
// 		logger.Errorf("close session for read: %v", err)
// 	}
// }

func (p *sessionPool) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// set flag to closed
	p.closed = true

	var err error

	for _, session := range p.sessions {
		err = session.checkpoint()
		if err != nil {
			logger.Errorf("wiredtiger: create checkpoint: %v", err)
		}
		err = session.close()
		if err != nil {
			logger.Errorf("close session: %v", err)
		}
	}

	return nil
}

type session struct {
	ref     *sessionPool
	impl    *C.WT_SESSION
	inuse   uint32
	closing int32
	cursors *cursorCache
}

func newSession(conn *C.WT_CONNECTION) (*session, error) {
	var sessionImpl *C.WT_SESSION

	configStr := C.CString("isolation=snapshot")
	defer C.free(unsafe.Pointer(configStr))

	result := int(C.wiredtiger_open_session(conn, nil, configStr, &sessionImpl))
	if checkError(result) {
		return nil, NewError(result)
	}

	s := &session{
		impl:  sessionImpl,
		inuse: 0,
	}

	s.cursors = newCursorCache()

	return s, nil
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
	atomic.StoreUint32(&s.inuse, 0)
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
