package node

import (
	"errors"
	"time"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/rpc"
)

var (
	ErrNodeStopped = errors.New("node not started")
	ErrNodeRunning = errors.New("node already running")
)

type Config struct {
	EnableApiTLS     bool
	TlsCrtFile       string
	TlsKeyFile       string
	DebugAPIAddr     string
	APIAddr          string
	DataDir          string
	IPCPath          string
	HTTPAddr         string
	HTTPCors         []string
	HTTPVirtualHosts []string
	HTTPModules      []string
	HTTPTimeouts     rpc.HTTPTimeouts
	HTTPPathPrefix   string
	WSAddr           string
	WSPathPrefix     string
	WSOrigins        []string
	WSModules        []string
}

// checkModuleAvailability checks that all names given in modules are actually
// available API services. It assumes that the MetadataApi module ("rpc") is always available;
// the registration of this "rpc" module happens in NewServer() and is thus common to all endpoints.
func checkModuleAvailability(modules []string, apis []rpc.API) (bad, available []string) {
	availableSet := make(map[string]struct{})
	for _, api := range apis {
		if _, ok := availableSet[api.Namespace]; !ok {
			availableSet[api.Namespace] = struct{}{}
			available = append(available, api.Namespace)
		}
	}
	for _, name := range modules {
		if _, ok := availableSet[name]; !ok && name != rpc.MetadataApi {
			bad = append(bad, name)
		}
	}
	return bad, available
}

// CheckTimeouts ensures that timeout values are meaningful
func CheckTimeouts(timeouts *rpc.HTTPTimeouts, log logging.Logger) {
	if timeouts.ReadTimeout < time.Second {
		log.Warning("Sanitizing invalid HTTP read timeout", "provided", timeouts.ReadTimeout, "updated", rpc.DefaultHTTPTimeouts.ReadTimeout)
		timeouts.ReadTimeout = rpc.DefaultHTTPTimeouts.ReadTimeout
	}
	if timeouts.WriteTimeout < time.Second {
		log.Warning("Sanitizing invalid HTTP write timeout", "provided", timeouts.WriteTimeout, "updated", rpc.DefaultHTTPTimeouts.WriteTimeout)
		timeouts.WriteTimeout = rpc.DefaultHTTPTimeouts.WriteTimeout
	}
	if timeouts.IdleTimeout < time.Second {
		log.Warning("Sanitizing invalid HTTP idle timeout", "provided", timeouts.IdleTimeout, "updated", rpc.DefaultHTTPTimeouts.IdleTimeout)
		timeouts.IdleTimeout = rpc.DefaultHTTPTimeouts.IdleTimeout
	}
}
