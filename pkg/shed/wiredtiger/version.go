package wiredtiger

/*
#cgo CFLAGS: -Ic:/wiredtiger/include
#cgo LDFLAGS: -Lc:/wiredtiger/lib -lwiredtiger

#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"

func Version() (version string, major int, minor int, patch int) {
	var a, b, c C.int

	version = C.GoString(C.wiredtiger_version(&a, &b, &c))
	major = int(a)
	minor = int(b)
	patch = int(c)

	return
}
