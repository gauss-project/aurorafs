package multiresolver

import "github.com/gauss-project/aurorafs/pkg/logging"

func GetLogger(mr *MultiResolver) logging.Logger {
	return mr.logger
}

func GetCfgs(mr *MultiResolver) []ConnectionConfig {
	return mr.cfgs
}
