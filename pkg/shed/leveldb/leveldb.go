package leveldb

import (
	"errors"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	m    *sync.RWMutex
	db   *leveldb.DB
	opts *opt.Options
	path string
}

func (l *LevelDB) Put(key driver.Key, value driver.Value) error {
	l.m.RLock()
	defer l.m.RUnlock()
	err := l.db.Put(key.Data, value.Data, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return nil
}

func (l *LevelDB) Get(key driver.Key) ([]byte, error) {
	l.m.RLock()
	defer l.m.RUnlock()
	value, err := l.db.Get(key.Data, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, driver.ErrNotFound
		}
		return nil, err
	}
	return value, nil
}

func (l *LevelDB) Has(key driver.Key) (bool, error) {
	l.m.RLock()
	defer l.m.RUnlock()
	yes, err := l.db.Has(key.Data, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return false, driver.ErrNotFound
		}
		return false, err
	}
	return yes, nil
}

func (l *LevelDB) Delete(key driver.Key) error {
	l.m.RLock()
	defer l.m.RUnlock()
	err := l.db.Delete(key.Data, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return nil
}

func (l *LevelDB) Search(query driver.Query) driver.Cursor {
	var rng *util.Range

	if query.MatchPrefix {
		rng = util.BytesPrefix(query.Prefix.Data)
	}

	i := l.db.NewIterator(rng, nil)

	i.Seek(query.Prefix.Data)

	return &Iterator{
		q: query,
		m: l.m,
		i: i,
	}
}

func (l *LevelDB) GetSnapshot() (driver.Snapshot, error) {
	s, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{Snapshot: s}, nil
}

type Snapshot struct {
	*leveldb.Snapshot
}

func (s *Snapshot) Get(key driver.Key) (data []byte, err error) {
	defer func() {
		if errors.Is(err, leveldb.ErrNotFound) {
			err = driver.ErrNotFound
			return
		}
	}()
	return s.Snapshot.Get(key.Data, nil)
}

func (s *Snapshot) Has(key driver.Key) (has bool, err error) {
	defer func() {
		if errors.Is(err, leveldb.ErrNotFound) {
			err = driver.ErrNotFound
			return
		}
	}()
	return s.Snapshot.Has(key.Data, nil)
}

func (s *Snapshot) Close() error {
	s.Snapshot.Release()
	return nil
}

type Iterator struct {
	i iterator.Iterator
	q driver.Query
	m *sync.RWMutex
}

func (i *Iterator) Last() bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Last()
}

func (i *Iterator) Error() error {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Error()
}

func (i *Iterator) Next() bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Next()
}

func (i *Iterator) Prev() bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Prev()
}

func (i *Iterator) Seek(key driver.Key) bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Seek(key.Data)
}

func (i *Iterator) Key() []byte {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Key()
}

func (i *Iterator) Value() []byte {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Value()
}

func (i *Iterator) Valid() bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.i.Valid()
}

func (i *Iterator) Close() error {
	i.m.RLock()
	defer i.m.RUnlock()
	i.i.Release()
	return nil
}

type Batch struct {
	m  *sync.RWMutex
	b  *leveldb.Batch
	db *leveldb.DB
}

func (b *Batch) Put(key driver.Key, value driver.Value) error {
	b.b.Put(key.Data, value.Data)
	return nil
}

func (b *Batch) Delete(key driver.Key) error {
	b.b.Delete(key.Data)
	return nil
}

func (b *Batch) Commit() error {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.db.Write(b.b, &opt.WriteOptions{Sync: false})
}

func (l *LevelDB) NewBatch() driver.Batching {
	return &Batch{
		b:  new(leveldb.Batch),
		m:  l.m,
		db: l.db,
	}
}

func (l *LevelDB) Close() error {
	l.m.Lock()
	defer l.m.Unlock()
	return l.db.Close()
}
