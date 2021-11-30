//go:build leveldb
// +build leveldb

package shed

import "github.com/gauss-project/aurorafs/pkg/shed/leveldb"

func init() {
	Register("leveldb", leveldb.Driver{})
}
