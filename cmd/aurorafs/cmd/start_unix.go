// +build !windows

package cmd

import (
	"errors"

	"github.com/gauss-project/aurorafs/pkg/logging"
)

func isWindowsService() (bool, error) {
	return false, nil
}

func createWindowsEventLogger(svcName string, logger logging.Logger) (logging.Logger, error) {
	return nil, errors.New("cannot create Windows event logger")
}
