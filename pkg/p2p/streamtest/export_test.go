package streamtest

import "time"

func SetFullCloseTimeout(t time.Duration) {
	fullCloseTimeout = t
}

func ResetFullCloseTimeout() {
	fullCloseTimeout = fullCloseTimeoutDefault
}
