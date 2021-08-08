package mockbytes

import "errors"

var ErrShortRead = errors.New("Not enough random bytes")
