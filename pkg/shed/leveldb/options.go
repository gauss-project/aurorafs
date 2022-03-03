package leveldb

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	optionBlockCacheCapacity     = "BlockCacheCapacity"
	optionOpenFilesCacheCapacity = "OpenFilesCacheCapacity"
	optionWriteBuffer            = "WriteBuffer"
	optionDisableSeeksCompaction = "DisableSeeksCompaction"
)

type Configuration opt.Options

func (c *Configuration) Options(opts ...driver.Option) map[string]struct{} {
	exported := make(map[string]struct{})

	for _, o := range opts {
		switch o.Identity() {
		case optionBlockCacheCapacity:
			c.BlockCacheCapacity = o.Value().(int)
		case optionOpenFilesCacheCapacity:
			c.OpenFilesCacheCapacity = o.Value().(int)
		case optionWriteBuffer:
			c.WriteBuffer = o.Value().(int)
		case optionDisableSeeksCompaction:
			c.DisableSeeksCompaction = o.Value().(bool)
		}

		if o.Exported() {
			exported[o.Identity()] = struct{}{}
		}
	}

	return exported
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

// SetDisableSeeksCompaction toggles the seek driven compactions feature on leveldb.
func (c *Configuration) SetDisableSeeksCompaction(b bool) driver.Option {
	o := driver.NewOption(optionDisableSeeksCompaction, false, true)
	o.Set(b)
	return o
}
