package leveldb

import (
	"errors"
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
	defaultBlockSize              = 256 * 1024
	defaultCompactionTableSize    = 128 * 1024 * 1024
	defaultCompactionTotalSize    = 5 * 128 * 1024 * 1024
	defaultOpenFilesLimit         = 256
	defaultBlockCacheCapacity     = 32 * 1024 * 1024
	defaultWriteBufferSize        = 32 * 1024 * 1024
	defaultDisableSeeksCompaction = false
)

func (d Driver) Init() driver.Configure {
	var c Configuration

	c.Options(
		c.SetBlockSize(defaultBlockSize),
		c.SetCompactionTableSize(defaultCompactionTableSize),
		c.SetCompactionTotalSize(defaultCompactionTotalSize),
		c.SetOpenFilesCacheCapacity(defaultOpenFilesLimit),
		c.SetBlockCacheCapacity(defaultBlockCacheCapacity),
		c.SetWriteBuffer(defaultWriteBufferSize),
		c.SetDisableSeeksCompaction(defaultDisableSeeksCompaction),
	)

	return &c
}

func (d Driver) Open(path string) (driver.DB, error) {
	i := d.Init().(*Configuration)

	var opts opt.Options
	if i != nil {
		opts = opt.Options(*i)
	}

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

	if _, err = ldb.getSchema(); err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			// Save schema with initialized default fields.
			if err = ldb.putSchema(schema{
				Fields:  make(map[string]driver.FieldSpec),
				Indexes: make(map[byte]driver.IndexSpec),
			}); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return ldb, nil
}
