package leveldb

import (
	"encoding/json"
	"reflect"
	"sync"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the shed/driver package.
type Driver struct{}

var (
	defaultOpenFilesLimit         = 256
	defaultBlockCacheCapacity     = 32 * 1024 * 1024
	defaultWriteBufferSize        = 32 * 1024 * 1024
	defaultDisableSeeksCompaction = false
)

func (d Driver) Open(path, options string) (driver.DB, error) {
	var c Configuration

	exported := c.Options(
		c.SetOpenFilesCacheCapacity(defaultOpenFilesLimit),
		c.SetBlockCacheCapacity(defaultBlockCacheCapacity),
		c.SetWriteBuffer(defaultWriteBufferSize),
		c.SetDisableSeeksCompaction(defaultDisableSeeksCompaction),
	)

	if options != "" {
		var override opt.Options
		err := json.Unmarshal([]byte(options), &override)
		if err != nil {
			return nil, err
		}
		rv := reflect.ValueOf(override)
		rt := reflect.TypeOf(override)
		cv := reflect.ValueOf(&c) // must be pointer
		for i := 0; i < rt.NumField(); i++ {
			f := rv.Field(i)
			name := rt.Field(i).Name
			if _, ok := exported[name]; ok {
				ft := cv.Elem().FieldByName(name)
				if ft.Kind() != reflect.Interface && !f.IsZero() {
					ft.Set(f)
				}
			}
		}
	}

	opts := opt.Options(c)

	var (
		err error
		db  *leveldb.DB
	)

	if path == "" {
		db, err = leveldb.Open(storage.NewMemStorage(), &opts)
	} else {
		db, err = leveldb.OpenFile(path, &opts)
		if dberrors.IsCorrupted(err) && !opts.GetReadOnly() {
			db, err = leveldb.RecoverFile(path, &opts)
		}
	}

	if err != nil {
		return nil, err
	}

	ldb := &LevelDB{
		m:    new(sync.RWMutex),
		db:   db,
		opts: &opts,
		path: path,
	}

	return ldb, nil
}
