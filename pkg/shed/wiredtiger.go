//go:build wiredtiger
// +build wiredtiger

package shed

import "github.com/gauss-project/aurorafs/pkg/shed/wiredtiger"

func init() {
	Register("wiredtiger", wiredtiger.Driver{})
}
