package leveldb

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	optionBlockSize              = "blockSize"
	optionBlockCacheCapacity     = "blockCacheCapacity"
	optionCompactionTableSize    = "compactionTableSize"
	optionCompactionTotalSize    = "compactionTotalSize"
	optionOpenFilesCacheCapacity = "openFilesCacheCapacity"
	optionWriteBuffer            = "writeBuffer"
	optionDisableSeeksCompaction = "disableSeeksCompaction"
)

type Configuration opt.Options

func (c *Configuration) Options(opts ...driver.Option) {
	for _, o := range opts {
		switch o.Identity() {
		case optionBlockSize:
			c.BlockSize = o.Value().(int)
		case optionBlockCacheCapacity:
			c.BlockCacheCapacity = o.Value().(int)
		case optionCompactionTableSize:
			c.CompactionTableSize = o.Value().(int)
		case optionCompactionTotalSize:
			c.CompactionTotalSize = o.Value().(int)
		case optionOpenFilesCacheCapacity:
			c.OpenFilesCacheCapacity = o.Value().(int)
		case optionDisableSeeksCompaction:
			c.DisableSeeksCompaction = o.Value().(bool)
		}
	}
}

func (c *Configuration) SetBlockSize(n int) driver.Option {
	o := driver.NewOption(optionBlockSize, int(0))
	o.Set(n)
	return o
}

// SetBlockCacheCapacity defines the block cache capacity.
func (c *Configuration) SetBlockCacheCapacity(n int) driver.Option {
	o := driver.NewOption(optionBlockCacheCapacity, int(0))
	o.Set(n)
	return o
}

func (c *Configuration) SetCompactionTableSize(n int) driver.Option {
	o := driver.NewOption(optionCompactionTableSize, int(0))
	o.Set(n)
	return o
}

func (c *Configuration) SetCompactionTotalSize(n int) driver.Option {
	o := driver.NewOption(optionCompactionTotalSize, int(0))
	o.Set(n)
	return o
}

// SetOpenFilesCacheCapacity defines the upper bound of open files that the
// localstore should maintain at any point of time.
func (c *Configuration) SetOpenFilesCacheCapacity(n int) driver.Option {
	o := driver.NewOption(optionOpenFilesCacheCapacity, int(0))
	o.Set(n)
	return o
}

// SetWriteBuffer defines the size of writer buffer.
func (c *Configuration) SetWriteBuffer(n int) driver.Option {
	o := driver.NewOption(optionWriteBuffer, int(0))
	o.Set(n)
	return o
}

// SetDisableSeeksCompaction toggles the seek driven compactions feature on leveldb.
func (c *Configuration) SetDisableSeeksCompaction(b bool) driver.Option {
	o := driver.NewOption(optionDisableSeeksCompaction, false)
	o.Set(b)
	return o
}
