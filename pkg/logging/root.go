package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

var (
	root = New(os.Stdout, logrus.TraceLevel)
)

func Root() Logger {
	return root
}

func Tracef(format string, args ...interface{}) {
	root.Tracef(format, args)
}

func Trace(args ...interface{}) {
	root.Trace(args)
}

func Debugf(format string, args ...interface{}) {
	root.Tracef(format, args)
}

func Debug(args ...interface{}) {
	root.Trace(args)
}

func Infof(format string, args ...interface{}) {
	root.Tracef(format, args)
}

func Info(args ...interface{}) {
	root.Trace(args)
}

func Warningf(format string, args ...interface{}) {
	root.Tracef(format, args)
}

func Warning(args ...interface{}) {
	root.Trace(args)
}

func Errorf(format string, args ...interface{}) {
	root.Tracef(format, args)
}

func Error(args ...interface{}) {
	root.Trace(args)
}
