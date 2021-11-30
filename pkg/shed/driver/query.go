package driver

import "errors"

type Key struct {
	Prefix int
	Data   []byte
}

type Value struct {
	Data []byte
}

type Query struct {
	Prefix Key
}

var ErrNotFound = errors.New("key not found")
