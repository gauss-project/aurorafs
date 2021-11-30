package wiredtiger

/*
#cgo LDFLAGS: -lwiredtiger
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
	"container/list"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var sessionMaxSize uint64

type sessionPoolStat struct {
	lived      uint64
	txnLived   uint64
	lastOpen   time.Time // locked by pool.lock
	lastCommit time.Time
}

type sessionPool struct {
	conn   *C.WT_CONNECTION
	list   *stack
	append *list.List
	lock   *sync.Mutex // lock append
	size   uint64
	stat   *sessionPoolStat
	id     uint64
	closed bool
}

type stack struct {
	top unsafe.Pointer
	len uint64
}

func NewStack() *stack {
	return &stack{}
}

type link struct {
	next unsafe.Pointer
	v    interface{}
}

func (s *stack) pop() interface{} {
	var top, next unsafe.Pointer
	var item *link
	for {
		top = atomic.LoadPointer(&s.top)
		if top == nil {
			return nil
		}
		item = (*link)(top)
		next = atomic.LoadPointer(&item.next)
		if atomic.CompareAndSwapPointer(&s.top, top, next) {
			atomic.AddUint64(&s.len, ^uint64(0))
			return item.v
		}
	}
}

// Push pushes a value on top of the stack.
func (s *stack) push(v interface{}) {
	item := link{v: v}
	var top unsafe.Pointer
	for {
		top = atomic.LoadPointer(&s.top)
		item.next = top
		if atomic.CompareAndSwapPointer(&s.top, top, unsafe.Pointer(&item)) {
			atomic.AddUint64(&s.len, 1)
			return
		}
	}
}

func newSessionPool(conn *C.WT_CONNECTION, size uint64) (*sessionPool, error) {
	p := &sessionPool{
		list: NewStack(),
		conn: conn,
		size: size,
		lock: new(sync.Mutex),
		stat: new(sessionPoolStat),
	}

	// init append list
	p.append = new(list.List)

	// clear session before now
	p.stat.lastOpen = time.Now()

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

func (p *sessionPool) dequeue() *session {
	p.lock.Lock()
	defer p.lock.Unlock()
	i := p.append.Back()
	if i == nil {
		return nil
	}
	s := i.Value.(*session)
	p.append.Remove(i)
	return s
}

func (p *sessionPool) enqueue(s *session, refresh bool) {
	if refresh {
		s.refresh()
	}
	s.overflow(1)
	p.lock.Lock()
	p.append.PushBack(s)
	if p.stat.lastOpen.Before(s.epoch) {
		p.stat.lastOpen = s.epoch
	}
	p.lock.Unlock()
}

func (p *sessionPool) evict() (removed uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	logger.Println("session pool evict start")
	for i := p.append.Front(); i != nil; i = i.Next() {
		e := i.Value.(*session)
		if time.Since(e.epoch) < gcPeriod {
			break
		}
		locked := atomic.LoadUint64(&e.lockId)
		if locked != 0 {
			continue
		}
		err := e.close()
		if err != nil {
			logger.Errorf("wiredtiger: session %d remove queue: %v", e.id, err)
		}
		p.append.Remove(i)
		removed++
	}
	if i := p.append.Front(); i != nil {
		p.stat.lastOpen = i.Value.(*session).epoch
	} else {
		p.stat.lastOpen = time.Now()
	}

	return
}

func (p *sessionPool) getLazy(locked uint64, check bool) *session {
	var (
		s   *session
		err error
	)
	l := atomic.LoadUint64(&p.stat.lived)
	fmt.Println("current session lived", l)
	if l < p.size {
		i := p.list.pop()
		if i == nil {
			logger.Println("create session")
			s, err = newSession(p.conn, p.getSessionID(), locked, false)
			if err != nil {
				logger.Errorf("wiredtiger: create session: %v", err)

				return p.get(locked)
			}
			p.list.push(s)
		} else {
			s = i.(*session)
			if atomic.LoadInt32(&s.closing) == 1 {
				return p.get(locked)
			}
			if atomic.LoadUint64(&s.lockId) != 0 {
				p.list.push(s)
				logger.Println("create session by locked")
				s, err = newSession(p.conn, p.getSessionID(), locked, false)
				if err != nil {
					logger.Errorf("wiredtiger: create session: %v", err)

					return p.get(locked)
				}
			}
		}
		atomic.AddUint64(&p.stat.lived, 1)
		return s
	}
	if l == p.size {
		// double check
		logger.Println("double check")
		if !check {
			return p.getLazy(locked, !check)
		}
	}
	if l == sessionMaxSize {
		logger.Warnf("wiredtiger: current session reach max size %d", sessionMaxSize)
		removed := p.evict()
		if removed > 0 {
			if !atomic.CompareAndSwapUint64(&p.stat.lived, l, l-removed) {
				return p.get(locked)
			}
		}
	}
	s = p.dequeue()
	if s == nil || atomic.LoadUint64(&s.lockId) != 0 {
		logger.Println("new oversize session")
		s, err = newSession(p.conn, p.getSessionID(), locked, true)
		if err != nil {
			logger.Errorf("wiredtiger: create session over list: %v", err)

			return p.get(locked)
		}
		p.enqueue(s, false)
		atomic.AddUint64(&p.stat.lived, 1)
	}
	if atomic.LoadInt32(&s.closing) == 1 {
		return p.get(locked)
	}
	return s
}

func (p *sessionPool) get(locked uint64) *session {
	if p.closed {
		return newFailSession(p.getSessionID(), locked, true)
	}
	return p.getLazy(locked, locked != 0)
}

func (p *sessionPool) Get() *session {
	// return p.get(0)
	s, _ := newSession(p.conn, p.getSessionID(), 0, false)
	return s
}

func (p *sessionPool) Lock(id uint64) *session {
	// atomic.AddUint64(&p.stat.txnLived, 1)
	// return p.get(id)
	s, _ := newSession(p.conn, p.getSessionID(), id, false)
	return s
}

const gcPeriod = 5 * time.Minute

func (p *sessionPool) put(s *session, lockId uint64) {
	var (
		lock bool
		fill bool
	)

	if lockId != 0 {
		if atomic.LoadUint64(&s.lockId) != lockId {
			lock = true
		} else if !atomic.CompareAndSwapUint64(&s.lockId, lockId, 0) {
			// because this session is putting in other goroutine
			return
		}
	}

	// check buffer size
	l := atomic.LoadUint64(&p.stat.lived)
	if l >= p.size {
		p.lock.Lock()
		lastOpen := p.stat.lastOpen
		p.lock.Unlock()
		if time.Since(lastOpen) > gcPeriod {
			removed := p.evict()
			if removed > 0 {
				if !atomic.CompareAndSwapUint64(&p.stat.lived, l, l-removed) {
					// retry put
					p.put(s, lockId)
					return
				}
			}
		}
	}

	// check buffer size again
	l = atomic.LoadUint64(&p.stat.lived)
	if l < p.size {
		fill = true
	}

	if lockId != 0 && !lock {
		// reset session stat
		err := s.reset()
		if err != nil {
			logger.Warnf("wiredtiger: session %d reset: %v", s.id, err)
		}
	}

	if !p.closed {
		overflowed := atomic.LoadInt32(&s.oversize)
		if overflowed == 1 {
			if !fill {
				p.enqueue(s, lock)
			} else {
				s.refresh()
				s.overflow(0)
				p.list.push(s)
			}
		} else {
			s.refresh()
			p.list.push(s)
		}

		if !lock {
			atomic.AddUint64(&p.stat.lived, 0)
		}

		return
	}

	// immediately close
	err := s.close()
	if err != nil {
		logger.Warnf("wiredtiger: session %d close on quit: %v", s.id, err)
	}
}

func (p *sessionPool) Put(s *session) {
	// p.put(s, 0)
	err := s.close()
	if err != nil {
		logger.Warnf("wiredtiger: session %d close on quit: %v", s.id, err)
	}
}

func (p *sessionPool) PutLock(s *session, lockId uint64) {
	// p.put(s, lockId)
	err := s.close()
	if err != nil {
		logger.Warnf("wiredtiger: session %d close on quit: %v", s.id, err)
	}
}

func (p *sessionPool) Close() error {
	var wg sync.WaitGroup

	// set flag to closed
	p.closed = true

	go func() {
		wg.Add(1)
		defer wg.Done()

		p.lock.Lock()
		if p.append.Len() > 0 {
			for i := p.append.Front(); i != nil; i = i.Next() {
				e := i.Value.(*session)
				if lockId := atomic.LoadUint64(&e.lockId); lockId != 0 {
					logger.Warnf("wiredtiger: tx session %d closing", lockId)
				}
				_ = e.close()
				p.append.Remove(i)
			}
		}
		p.lock.Unlock()
	}()

	go func() {
		wg.Add(1)
		defer wg.Done()

		for {
			i := p.list.pop()
			if i == nil {
				break
			}
			e := i.(*session)
			if lockId := atomic.LoadUint64(&e.lockId); lockId != 0 {
				logger.Warnf("wiredtiger: tx session %d closing", lockId)
			}
			_ = e.close()
		}
	}()

	wg.Wait()

	return nil
}

type session struct {
	impl     *C.WT_SESSION
	lockId   uint64
	id       uint64
	oversize int32
	closing  int32
	txn      bool // set by newTxn
	cursors  *cursorCache
	epoch    time.Time
}

func newSession(conn *C.WT_CONNECTION, id, locked uint64, overflow bool) (*session, error) {
	var sessionImpl *C.WT_SESSION

	configStr := C.CString("isolation=snapshot")
	defer C.free(unsafe.Pointer(configStr))

	result := int(C.wiredtiger_open_session(conn, nil, configStr, &sessionImpl))
	if checkError(result) {
		return nil, NewError(result)
	}

	s := &session{
		impl:  sessionImpl,
		id:    id,
		epoch: time.Now(),
	}

	runtime.SetFinalizer(s, func(impl *session) { _ = impl.close() })
	runtime.KeepAlive(s)

	if locked != 0 {
		atomic.StoreUint64(&s.lockId, locked)
	}

	if overflow {
		atomic.StoreInt32(&s.oversize, 1)
	}

	s.cursors = newCursorCache(100)

	return s, nil
}

func newFailSession(id, locked uint64, overflow bool) *session {
	s := &session{
		id:      id,
		epoch:   time.Time{},
		closing: 1, // mark session closing
	}

	if locked != 0 {
		atomic.StoreUint64(&s.lockId, locked)
	}

	if overflow {
		atomic.StoreInt32(&s.oversize, 1)
	}

	return s
}

func (s *session) strerror(code int) error {
	err := C.wiredtiger_session_strerror(s.impl, C.int(code))
	if err != nil {
		// defer C.free(unsafe.Pointer(err))
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
	Log               struct {
		Enabled bool
	}
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

	result := int(C.wiredtiger_session_compact(s.impl, objStr, configStr))
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
	return s.cursors.newCursor(s, obj.String(), opt)
}

func (s *session) refresh() {
	s.epoch = time.Now()
}

func (s *session) overflow(m int) {
	atomic.StoreInt32(&s.oversize, int32(m))
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
