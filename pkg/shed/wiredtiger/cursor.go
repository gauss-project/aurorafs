package wiredtiger

/*
#cgo LDFLAGS: -lwiredtiger
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
	"unsafe"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

type cursorCache struct {
	size    uint64
	count   int
	index   uint64
	cursors *list.List
}

func newCursorCache(size uint64) *cursorCache {
	return &cursorCache{
		size:    size,
		cursors: new(list.List),
	}
}

type cursorOption struct {
	Raw       bool
	ReadOnce  bool
	Overwrite bool
}

func (cc *cursorCache) newCursor(s *session, uri string, opt *cursorOption) (*cursor, error) {
	cc.count++

	// try to find from cache first
	if !opt.ReadOnce {
		for i := cc.cursors.Front(); i != nil; i = i.Next() {
			c := i.Value.(*cursor)
			if c.uri == uri {
				cc.cursors.Remove(i)
				return c, nil
			}
		}
	}

	c, err := openCursor(s, uri, structToList(*opt, false))
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (cc *cursorCache) releaseCursor(c *cursor) {
	cc.count--

	err := c.reset()
	if err != nil {
		logger.Errorf("wiredtiger: release cursor(%s#%d): %v", c.uri, c.s.id, err)
	}

	cc.index++
	c.gen = cc.index
	cc.cursors.PushFront(c)

	for cc.cursors.Len() > 0 {
		i := cc.cursors.Back()
		if i != nil {
			c := i.Value.(*cursor)
			if cc.index-c.gen > cc.size {
				cc.cursors.Remove(i)
				err = c.close()
				if err != nil {
					logger.Errorf("wiredtiger: clean old cursor(%s#%d): %v", c.uri, c.s.id, err)
				}
			}
		}
	}
}

func (cc *cursorCache) closeAll(uri string) error {
	all := uri == ""

	var errRet error

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
	typ  dataType
	gen  uint64
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
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyData := unsafe.Pointer(&key[0])
	keySize := C.size_t(len(key))

	if len(value) == 0 {
		return ErrInvalidArgument
	}

	valueData := unsafe.Pointer(&value[0])
	valueSize := C.size_t(len(value))

	result := int(C.wiredtiger_cursor_insert(c.impl, keyData, keySize, valueData, valueSize))
	if checkError(result) {
		return NewError(result, c.s)
	}
	return nil
}

func (c *cursor) update(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyData := unsafe.Pointer(&key[0])
	keySize := C.size_t(len(key))

	defer C.free(keyData)

	if len(value) == 0 {
		return ErrInvalidArgument
	}

	valueData := unsafe.Pointer(&value[0])
	valueSize := C.size_t(len(value))

	defer C.free(valueData)

	result := int(C.wiredtiger_cursor_update(c.impl, keyData, keySize, valueData, valueSize))
	if checkError(result) {
		return NewError(result, c.s)
	}
	return nil
}

func (c *cursor) find(key []byte) (*resultCursor, error) {
	if len(key) == 0 {
		return nil, ErrInvalidArgument
	}

	data := unsafe.Pointer(&key[0])
	size := C.size_t(len(key))

	result := int(C.wiredtiger_cursor_search(c.impl, data, size))
	if checkError(result) {
		return nil, NewError(result, c.s)
	}

	return &resultCursor{cursor: c, k: key}, nil
}

func (c *cursor) search(key []byte) (*searchCursor, error) {
	var compare C.int
	var data unsafe.Pointer
	var size C.size_t = 0

	if len(key) != 0 {
		data = unsafe.Pointer(&key[0])
		size = C.size_t(len(key))

		result := int(C.wiredtiger_cursor_search_near(c.impl, data, size, &compare))
		if checkError(result) {
			return nil, NewError(result, c.s)
		}
	}

	return &searchCursor{cursor: c, k: key, direct: int(compare)}, nil
}

func (c *cursor) remove(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	data := unsafe.Pointer(&key[0])
	size := C.size_t(len(key))

	result := int(C.wiredtiger_cursor_remove(c.impl, data, size))
	if checkError(result) {
		return NewError(result, c.s)
	}

	return nil
}

func (c *cursor) key() ([]byte, error) {
	var item C.WT_ITEM

	result := int(C.wiredtiger_cursor_get_key(c.impl, &item))
	if checkError(result) {
		return nil, NewError(result, c.s)
	}

	return C.GoBytes(item.data, C.int(item.size)), nil
}

func (c *cursor) value() ([]byte, error) {
	var item C.WT_ITEM

	result := int(C.wiredtiger_cursor_get_value(c.impl, &item))
	if checkError(result) {
		return nil, NewError(result, c.s)
	}

	return C.GoBytes(item.data, C.int(item.size)), nil
}

func (c *cursor) reset() error {
	result := int(C.wiredtiger_cursor_reset(c.impl))
	if checkError(result) {
		return NewError(result, c.s)
	}
	return nil
}

func (c *cursor) close() error {
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
	return r.reset()
}

type searchCursor struct {
	*cursor
	k      []byte
	err    error
	direct int // search near flag
}

func (s *searchCursor) Error() error {
	return s.err
}

func (s *searchCursor) Valid() bool {
	return s.exit && s.err != nil
}

func (s *searchCursor) Last() bool {
	err := s.reset()
	if err != nil {
		s.err = err
		return false
	}

	return s.Prev()
}

func (s *searchCursor) Next() bool {
	result := int(C.wiredtiger_cursor_next(s.impl))
	if checkError(result) {
		err := NewError(result)
		if !IsNotFound(err) {
			s.err = fmt.Errorf("wiredtiger: cursor walk forward for key %s: %v", s.k, err)
		}

		return false
	}

	return true
}

func (s *searchCursor) Prev() bool {
	result := int(C.wiredtiger_cursor_prev(s.impl))
	if checkError(result) {
		err := NewError(result)
		if !IsNotFound(err) {
			s.err = fmt.Errorf("wiredtiger: cursor walk backward for key %s: %v", s.k, err)
		}

		return false
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

	c, err := s.search(k)
	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor seek key %s: %v", k, err)
		return false
	}

	s.k = c.k
	s.cursor = c.cursor
	s.direct = c.direct

	return s.direct < 0
}

func (s *searchCursor) Key() []byte {
	data, err := s.key()
	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor search key %s: %v", s.k, err)

		return nil
	}
	return data
}

func (s *searchCursor) Value() []byte {
	data, err := s.value()
	if err != nil {
		s.err = fmt.Errorf("wiredtiger: cursor search value for key %s: %v", s.k, err)

		return nil
	}
	return data
}

func (s *searchCursor) Close() error {
	return s.reset()
}

type invalidCursor struct{}

var InvalidCursor = &invalidCursor{}

func (i *invalidCursor) Valid() bool {
	return false
}

func (i *invalidCursor) Next() bool {
	return false
}

func (i *invalidCursor) Prev() bool {
	return false
}

func (i *invalidCursor) Last() bool {
	return false
}

func (i *invalidCursor) Seek(_ driver.Key) bool {
	return false
}

func (i *invalidCursor) Key() []byte {
	return nil
}

func (i *invalidCursor) Value() []byte {
	return nil
}

func (i *invalidCursor) Error() error {
	return fmt.Errorf("wiredtiger: move to invalid cursor")
}

func (i *invalidCursor) Close() error {
	return nil
}
