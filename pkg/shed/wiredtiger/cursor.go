package wiredtiger

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lwiredtiger
#include <stdlib.h>
#include <wiredtiger.h>

int wiredtiger_session_open_cursor(WT_SESSION *session, const char *uri, WT_CURSOR *to_dup, const char *config, WT_CURSOR **cursorp) {
	int ret = session->open_cursor(session, uri, to_dup, config, cursorp);
	if(ret)
		return ret;
	if (((*cursorp)->flags & WT_CURSTD_DUMP_JSON) == 0)
		(*cursorp)->flags |= WT_CURSTD_RAW;
	return 0;
}

int wiredtiger_cursor_get_key(WT_CURSOR *cursor, WT_ITEM *v) {
	return cursor->get_key(cursor, v);
}

int wiredtiger_cursor_get_value(WT_CURSOR *cursor, WT_ITEM *v) {
	return cursor->get_value(cursor, v);
}

int wiredtiger_cursor_next(WT_CURSOR *cursor) {
	return cursor->next(cursor);
}

int wiredtiger_cursor_prev(WT_CURSOR *cursor) {
	return cursor->prev(cursor);
}

int wiredtiger_cursor_search(WT_CURSOR *cursor, const void *data, size_t size) {
	if (size != 0) {
		WT_ITEM key;
		key.data = data;
		key.size = size;
		cursor->set_key(cursor, &key);
	}
	return cursor->search(cursor);
}

int wiredtiger_cursor_search_near(WT_CURSOR *cursor, const void *data, size_t size, int *exactp) {
	if (size != 0) {
		WT_ITEM key;
		key.data = data;
		key.size = size;
		cursor->set_key(cursor, &key);
	}
	return cursor->search_near(cursor, exactp);
}

int wiredtiger_cursor_insert(WT_CURSOR *cursor, const void *key_data, size_t key_size, const void *val_data, size_t val_size) {
	if (key_size != 0) {
		WT_ITEM key;
		key.data = key_data;
		key.size = key_size;
		cursor->set_key(cursor, &key);
	}
	if (val_size != 0) {
		WT_ITEM value;
		value.data = val_data;
		value.size = val_size;
		cursor->set_value(cursor, &value);
	}
	return cursor->insert(cursor);
}

int wiredtiger_cursor_update(WT_CURSOR *cursor, const void *key_data, size_t key_size, const void *val_data, size_t val_size) {
	if (key_size != 0) {
		WT_ITEM key;
		key.data = key_data;
		key.size = key_size;
		cursor->set_key(cursor, &key);
	}
	if (val_size != 0) {
		WT_ITEM value;
		value.data = val_data;
		value.size = val_size;
		cursor->set_value(cursor, &value);
	}
	return cursor->update(cursor);
}

int wiredtiger_cursor_remove(WT_CURSOR *cursor, const void *data, size_t size) {
	if (size != 0) {
		WT_ITEM key;
		key.data = data;
		key.size = size;
		cursor->set_key(cursor, &key);
	}
	return cursor->remove(cursor);
}

int wiredtiger_cursor_reset(WT_CURSOR *cursor) {
	return cursor->reset(cursor);
}

int wiredtiger_cursor_close(WT_CURSOR *cursor) {
	return cursor->close(cursor);
}
*/
import "C"
import (
	"container/list"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type cursorCache struct {
	lock    sync.RWMutex
	cursors *list.List
}

func newCursorCache() *cursorCache {
	return &cursorCache{
		cursors: new(list.List),
	}
}

type cursorOption struct {
	Bulk      bool `key:"-"`
	Raw       bool
	ReadOnce  bool
	Overwrite bool
}

func (cc *cursorCache) newCursor(s *session, uri string, opt *cursorOption) (*cursor, error) {
	// try to find from cache first
	cc.lock.RLock()
	for i := cc.cursors.Front(); i != nil; i = i.Next() {
		c := i.Value.(*cursor)
		if c.uri == uri {
			cc.lock.RUnlock()
			err := c.reset()
			if err != nil {
				logger.Errorf("wiredtiger: release cursor(%s#%d): %v", c.uri, c.s.id, err)
			}
			return c, nil
		}
	}
	cc.lock.RUnlock()

	cc.lock.Lock()
	defer cc.lock.Unlock()
	c, err := openCursor(s, uri, structToList(*opt, false))
	if err != nil {
		return nil, err
	}

	cc.cursors.PushFront(c)

	return c, nil
}

func (cc *cursorCache) releaseCursor(c *cursor) {
	// TODO
}

func (cc *cursorCache) closeAll(uri string) error {
	all := uri == ""

	var errRet error

	cc.lock.Lock()
	defer cc.lock.Unlock()
	for i := cc.cursors.Front(); i != nil; i = i.Next() {
		c := i.Value.(*cursor)
		if all || c.uri == uri {
			err := c.close()
			if err != nil {
				errRet = err
			}
			cc.cursors.Remove(i)
		}
	}

	return errRet
}

type cursor struct {
	s    *session
	impl *C.WT_CURSOR
	lock sync.RWMutex
	typ  dataType
	uri  string
	kf   string // key format
	vf   string // value format
	exit bool
}

func openCursor(s *session, uri, config string) (*cursor, error) {
	var wc *C.WT_CURSOR
	var configStr *C.char = nil

	uriStr := C.CString(uri)
	defer C.free(unsafe.Pointer(uriStr))

	if len(config) > 0 {
		configStr = C.CString(config)
		defer C.free(unsafe.Pointer(configStr))
	}

	result := int(C.wiredtiger_session_open_cursor(s.impl, uriStr, nil, configStr, &wc))
	if checkError(result) {
		return nil, NewError(result, s)
	}

	c := &cursor{
		s:    s,
		impl: wc,
		uri:  uri,
		kf:   C.GoString(wc.key_format),
		vf:   C.GoString(wc.value_format),
	}

	return c, nil
}

func (c *cursor) insert(key, value []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyData := C.CBytes(key) // unsafe.Pointer
	keySize := C.size_t(len(key))

	defer C.free(keyData)

	if len(value) == 0 {
		return ErrInvalidArgument
	}

	valueData := C.CBytes(value) // unsafe.Pointer
	valueSize := C.size_t(len(value))

	defer C.free(valueData)

	result := int(C.wiredtiger_cursor_insert(c.impl, keyData, keySize, valueData, valueSize))
	if checkError(result) {
		return NewError(result, c.s)
	}
	return nil
}

func (c *cursor) update(key, value []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyData := C.CBytes(key) // unsafe.Pointer
	keySize := C.size_t(len(key))

	defer C.free(keyData)

	if len(value) == 0 {
		return ErrInvalidArgument
	}

	valueData := C.CBytes(value) // unsafe.Pointer
	valueSize := C.size_t(len(value))

	defer C.free(valueData)

	result := int(C.wiredtiger_cursor_update(c.impl, keyData, keySize, valueData, valueSize))
	if checkError(result) {
		return NewError(result, c.s)
	}
	return nil
}

func (c *cursor) find(key []byte) (*resultCursor, error) {
	c.lock.RLock()

	if len(key) == 0 {
		c.lock.RUnlock()
		return nil, ErrNotFound
	}

	data := C.CBytes(key) // unsafe.Pointer
	size := C.size_t(len(key))

	defer C.free(data)

	result := int(C.wiredtiger_cursor_search(c.impl, data, size))
	if checkError(result) {
		c.lock.RUnlock()
		return nil, NewError(result)
	}

	return &resultCursor{cursor: c, k: key}, nil
}

func (c *cursor) search(key []byte) (s *searchCursor, err error) {
	var compare C.int
	var data unsafe.Pointer
	var size C.size_t = 0

	if len(key) == 0 {
		key = []byte{0}
	}

	c.lock.RLock()

	data = C.CBytes(key)
	size = C.size_t(len(key))

	defer C.free(data)

	result := int(C.wiredtiger_cursor_search_near(c.impl, data, size, &compare))
	if checkError(result) {
		c.lock.RUnlock()
		return nil, NewError(result)
	}

	s = &searchCursor{
		k:      key,
		cursor: c,
		match:  int(compare) == 0,
		direct: int(compare),
	}

	return
}

func (c *cursor) remove(key []byte) error {
	c.lock.Lock()

	if len(key) == 0 {
		c.lock.Unlock()
		return ErrInvalidArgument
	}

	data := C.CBytes(key) // unsafe.Pointer
	size := C.size_t(len(key))

	defer C.free(data)

	result := int(C.wiredtiger_cursor_remove(c.impl, data, size))
	if checkError(result) {
		c.lock.Unlock()
		return NewError(result, c.s)
	}

	c.lock.Unlock()
	return nil
}

// key: Must be called by locked-state
func (c *cursor) key() ([]byte, error) {
	var item C.WT_ITEM

	result := int(C.wiredtiger_cursor_get_key(c.impl, &item))
	if checkError(result) {
		return nil, NewError(result, c.s)
	}

	return C.GoBytes(item.data, C.int(item.size)), nil
}

// value: Must be called by locked-state
func (c *cursor) value() ([]byte, error) {
	var item C.WT_ITEM

	result := int(C.wiredtiger_cursor_get_value(c.impl, &item))
	if checkError(result) {
		return nil, NewError(result, c.s)
	}

	return C.GoBytes(item.data, C.int(item.size)), nil
}

func (c *cursor) reset() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	result := int(C.wiredtiger_cursor_reset(c.impl))
	if checkError(result) {
		return NewError(result, c.s)
	}

	return nil
}

func (c *cursor) close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.exit {
		c.exit = true
		result := int(C.wiredtiger_cursor_close(c.impl))
		if checkError(result) {
			return NewError(result, c.s)
		}
	}
	return nil
}

type resultCursor struct {
	*cursor
	k   []byte
	err error
}

func (r *resultCursor) Valid() bool {
	return r.exit
}

func (r *resultCursor) Next() bool {
	return false
}

func (r *resultCursor) Prev() bool {
	return false
}

func (r *resultCursor) Last() bool {
	return true
}

func (r *resultCursor) Seek(_ driver.Key) bool {
	return false
}

func (r *resultCursor) Key() []byte {
	data, err := r.key()
	if err != nil {
		r.err = fmt.Errorf("wiredtiger: cursor find key %s: %v", r.k, err)

		return nil
	}
	return data
}

func (r *resultCursor) Value() []byte {
	data, err := r.value()
	if err != nil {
		r.err = fmt.Errorf("wiredtiger: cursor find value for key %s: %v", r.k, err)

		return nil
	}
	return data
}

func (r *resultCursor) Error() error {
	return r.err
}

func (r *resultCursor) Close() error {
	r.cursor.lock.RUnlock()
	r.s.cursors.releaseCursor(r.cursor)

	return nil
}

type searchCursor struct {
	*cursor
	prefix []byte
	k      []byte
	err    error
	match  bool
	direct int // search near flag
}

func (s *searchCursor) Error() error {
	return s.err
}

func (s *searchCursor) Valid() bool {
	return !s.exit && s.err == nil
}

func (s *searchCursor) Last() bool {
	s.cursor.lock.RUnlock()
	err := s.reset()
	if err != nil {
		s.err = err
		return false
	}

	s.cursor.lock.RLock()
	return s.Prev()
}

func (s *searchCursor) Next() bool {
	if s.match {
		s.match = false
	} else {
		result := int(C.wiredtiger_cursor_next(s.impl))
		if checkError(result) {
			err := NewError(result)
			if !IsNotFound(err) {
				s.err = fmt.Errorf("wiredtiger: cursor walk forward for key %s: %v", s.k, err)
			}

			return false
		}
	}

	return true
}

func (s *searchCursor) Prev() bool {
	if s.match {
		s.match = false
	} else {
		result := int(C.wiredtiger_cursor_prev(s.impl))
		if checkError(result) {
			err := NewError(result)
			if !IsNotFound(err) {
				s.err = fmt.Errorf("wiredtiger: cursor walk backward for key %s: %v", s.k, err)
			}

			return false
		}
	}

	return true
}

func (s *searchCursor) Seek(key driver.Key) bool {
	// check prefix
	table := strings.SplitN(s.uri, ":", 2)[1]
	obj, k := parseKey(key)
	switch obj.sourceName {
	case stateTableName:
		if table != stateTableName {
			return false
		}
	case fieldTableName:
		if table != fieldTableName {
			return false
		}
	default:
		if !strings.HasPrefix(obj.sourceName, indexTablePrefix) {
			s.err = fmt.Errorf("wiredtiger: parse unknown key type: %s", k)
			return false
		}
		if table != obj.sourceName {
			s.err = fmt.Errorf("wiredtiger: expect to find table %s, not %s", table, obj.sourceName)
			return false
		}
	}

	s.cursor.lock.RUnlock()
	c, err := s.search(k)

	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor seek key %s: %v", k, err)
		return false
	}

	s.k = c.k
	s.cursor = c.cursor
	s.direct = c.direct

	return true
}

func (s *searchCursor) Key() []byte {
	s.match = false
	key, err := s.key()
	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor search key %s: %v", s.k, err)
	}
	data := make([]byte, len(s.prefix)+len(key))
	copy(data, s.prefix)
	copy(data[len(s.prefix):], key)
	return data
}

func (s *searchCursor) Value() []byte {
	s.match = false
	data, err := s.value()
	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor search value for key %s: %v", s.k, err)
	}
	return data
}

func (s *searchCursor) Close() error {
	s.cursor.lock.RUnlock()
	s.s.cursors.releaseCursor(s.cursor)

	// put session
	s.s.ref.Put(s.s)

	return nil
}

type errorCursor struct {
	err error
}

var InvalidCursor = &errorCursor{}

func (e *errorCursor) Valid() bool {
	return false
}

func (e *errorCursor) Next() bool {
	return false
}

func (e *errorCursor) Prev() bool {
	return false
}

func (e *errorCursor) Last() bool {
	return false
}

func (e *errorCursor) Seek(_ driver.Key) bool {
	return false
}

func (e *errorCursor) Key() []byte {
	return nil
}

func (i *errorCursor) Value() []byte {
	return nil
}

func (e *errorCursor) Error() error {
	if e.err != nil {
		return fmt.Errorf("wiredtiger: cursor error: %s", e.err)
	}

	return nil
}

func (e *errorCursor) Close() error {
	return nil
}
