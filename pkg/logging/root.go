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
	root.Tracef(format, args...)
}

func Trace(args ...interface{}) {
	root.Trace(args)
}

func Debugf(format string, args ...interface{}) {
	root.Debugf(format, args...)
}

func Debug(args ...interface{}) {
	root.Debug(args)
}

func Infof(format string, args ...interface{}) {
	root.Infof(format, args...)
}

func Info(args ...interface{}) {
	root.Info(args)
}

func Warningf(format string, args ...interface{}) {
	root.Warningf(format, args...)
}

func Warning(args ...interface{}) {
	root.Warning(args)
}

func Errorf(format string, args ...interface{}) {
	root.Errorf(format, args...)
}

func Error(args ...interface{}) {
	root.Error(args)
}
