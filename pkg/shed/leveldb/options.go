package leveldb

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	optionBlockSize              = "BlockSize"
	optionBlockCacheCapacity     = "BlockCacheCapacity"
	optionOpenFilesCacheCapacity = "OpenFilesCacheCapacity"
	optionWriteBuffer            = "WriteBuffer"
	optionCompactionTableSize    = "CompactionTableSize"
	optionCompactionTotalSize    = "CompactionTotalSize"
	optionDisableSeeksCompaction = "DisableSeeksCompaction"
	optionNoSync                 = "NoSync"
)

type Configuration opt.Options

func (c *Configuration) Options(opts ...driver.Option) map[string]struct{} {
	exported := make(map[string]struct{})

	for _, o := range opts {
		switch o.Identity() {
		case optionBlockSize:
			c.BlockSize = o.Value().(int)
		case optionBlockCacheCapacity:
			c.BlockCacheCapacity = o.Value().(int)
		case optionOpenFilesCacheCapacity:
			c.OpenFilesCacheCapacity = o.Value().(int)
		case optionWriteBuffer:
			c.WriteBuffer = o.Value().(int)
		case optionCompactionTableSize:
			c.CompactionTableSize = o.Value().(int)
		case optionCompactionTotalSize:
			c.CompactionTotalSize = o.Value().(int)
		case optionDisableSeeksCompaction:
			c.DisableSeeksCompaction = o.Value().(bool)
		case optionNoSync:
			c.NoSync = o.Value().(bool)
		}

		if o.Exported() {
			exported[o.Identity()] = struct{}{}
		}
	}

	return exported
}

// SetBlockSize defines the minimum uncompressed size of each sorted table.
func (c *Configuration) SetBlockSize(n int) driver.Option {
	o := driver.NewOption(optionBlockSize, int(0), true)
	o.Set(n)
	return o
}

// SetBlockCacheCapacity defines the block cache capacity.
func (c *Configuration) SetBlockCacheCapacity(n int) driver.Option {
	o := driver.NewOption(optionBlockCacheCapacity, int(0), true)
	o.Set(n)
	return o
}

// SetOpenFilesCacheCapacity defines the upper bound of open files that the
// localstore should maintain at any point of time.
func (c *Configuration) SetOpenFilesCacheCapacity(n int) driver.Option {
	o := driver.NewOption(optionOpenFilesCacheCapacity, int(0), true)
	o.Set(n)
	return o
}

// SetWriteBuffer defines the size of writer buffer.
func (c *Configuration) SetWriteBuffer(n int) driver.Option {
	o := driver.NewOption(optionWriteBuffer, int(0), true)
	o.Set(n)
	return o
}

// SetCompactionTableSize limits size of sorted table.
func (c *Configuration) SetCompactionTableSize(n int) driver.Option {
	o := driver.NewOption(optionCompactionTableSize, int(0), true)
	o.Set(n)
	return o
}

// SetCompactionTotalSize limits total size of sorted table for each level.
func (c *Configuration) SetCompactionTotalSize(n int) driver.Option {
	o := driver.NewOption(optionCompactionTotalSize, int(0), true)
	o.Set(n)
	return o
}

// SetDisableSeeksCompaction toggles the seek driven compactions feature on leveldb.
func (c *Configuration) SetDisableSeeksCompaction(b bool) driver.Option {
	o := driver.NewOption(optionDisableSeeksCompaction, false, true)
	o.Set(b)
	return o
}

// SetNoSync allows completely disable fsync on leveldb.
func (c *Configuration) SetNoSync(b bool) driver.Option {
	o := driver.NewOption(optionNoSync, false, true)
	o.Set(b)
	return o
}
