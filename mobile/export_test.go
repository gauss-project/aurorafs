package mobile

import (
	"math/rand"
	"os"

	"github.com/gauss-project/aurorafs/pkg/node"
)

func ExportOptions(o *Options) node.Options {
	return o.export()
}

// export defaultConfig for test
func ExportDefaultConfig() (o *Options, err error) {
	o = defaultOptions

	// random listen port
	o.APIPort = rand.Intn(1234) + 23000
	o.P2PPort = o.APIPort + 1
	o.DebugAPIPort = o.P2PPort + 1

	o.DataPath, err = os.MkdirTemp(os.TempDir(), "aurora_test")
	if err != nil {
		return nil, err
	}

	// put keys into data dir
	o.KeysPath = o.DataPath

	// dev chain
	o.ContractAddress = "0x7F578e5ade91A30aC8ABf120d102E282821bd142"
	o.ChainEndpoint = "https://data-seed-prebsc-1-s1.binance.org:8545"

	return
}
