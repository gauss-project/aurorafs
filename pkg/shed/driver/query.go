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
	Prefix      Key
	MatchPrefix bool
}

var ErrNotFound = errors.New("key not found")
