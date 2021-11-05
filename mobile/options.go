package mobile

import (
	"fmt"
	"reflect"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/node"
)

// Options represents the collection of configuration values to fine tune the aurora
// node embedded into a mobile process. The available values are a subset of the
// entire API provided by aurora to reduce the maintenance surface and dev
// complexity.
type Options struct {
	// api setting
	APIPort        int
	DebugAPIPort   int
	EnableDebugAPI bool

	// p2p setup
	NetworkID      uint64
	P2PPort        int
	WelcomeMessage string

	// kademlia
	BinMaxPeers   int
	LightMaxPeers int

	// cache size
	CacheCapacity uint64

	// node bootstrap
	BootNodes      []string
	EnableDevNode  bool
	EnableFullNode bool

	// oracle setting
	ContractAddress string
	ChainEndpoint   string

	// security
	Password string
	KeysPath string
	DataPath string

	// leveldb opts
	BlockCacheCapacity     uint64
	OpenFilesLimit         uint64
	WriteBufferSize        uint64
	DisableSeeksCompaction bool

	// misc
	Verbosity string
}

// defaultOptions contains the default node configuration values to use if all
// or some fields are missing from the user's specified list.
var defaultOptions = &Options{
	APIPort:            1633,
	DebugAPIPort:       1635,
	P2PPort:            1634,
	CacheCapacity:      4000,
	EnableFullNode:     false,
	BinMaxPeers:        20,
	LightMaxPeers:      100,
	BlockCacheCapacity: 8 * 1024 * 1024,
	OpenFilesLimit:     1000,
	WriteBufferSize:    4 * 1024 * 1024,
	Verbosity:          "info",
}

const ListenAddress = "localhost"

func (o Options) DataDir(c *node.Options) {
	c.DataDir = o.DataPath
}

func (o Options) APIAddr(c *node.Options) {
	c.APIAddr = fmt.Sprintf("%s:%d", ListenAddress, o.APIPort)
}

func (o Options) DebugAPIAddr(c *node.Options) {
	if o.EnableDebugAPI {
		c.DebugAPIAddr = fmt.Sprintf("%s:%d", ListenAddress, o.DebugAPIPort)
	}
}

func (o Options) Bootnodes(c *node.Options) {
	c.Bootnodes = o.BootNodes
}

func (o Options) IsDev(c *node.Options) {
	c.IsDev = o.EnableDevNode
}

func (o Options) FullNode(c *node.Options) {
	if o.EnableFullNode {
		c.NodeMode.SetMode(aurora.FullNode)
	}
}

func (o Options) KadBinMaxPeers(c *node.Options) {
	c.KadBinMaxPeers = o.BinMaxPeers
}

func (o Options) LightNodeMaxPeers(c *node.Options) {
	c.LightNodeMaxPeers = o.LightMaxPeers
}

func (o Options) OracleContractAddress(c *node.Options) {
	c.OracleContractAddress = o.ContractAddress
}

func (o Options) OracleEndpoint(c *node.Options) {
	c.OracleEndpoint = o.ChainEndpoint
}

func (o Options) DBBlockCacheCapacity(c *node.Options) {
	c.DBBlockCacheCapacity = o.BlockCacheCapacity
}

func (o Options) DBOpenFilesLimit(c *node.Options) {
	c.DBOpenFilesLimit = o.OpenFilesLimit
}

func (o Options) DBWriteBufferSize(c *node.Options) {
	c.DBWriteBufferSize = o.WriteBufferSize
}

func (o Options) DBDisableSeeksCompaction(c *node.Options) {
	c.DBDisableSeeksCompaction = o.DisableSeeksCompaction
}

// Export exports Options to node.Options, skipping all other extra fields
func (o *Options) Export() (c node.Options) {
	localVal := reflect.ValueOf(o).Elem()
	remotePtr := reflect.ValueOf(&c)
	remoteVal := reflect.ValueOf(&c).Elem()
	remoteType := reflect.TypeOf(&c).Elem()

	for i := 0; i < remoteVal.NumField(); i++ {
		remoteFieldVal := remoteVal.Field(i)
		localFieldVal := localVal.FieldByName(remoteType.Field(i).Name)
		if reflect.ValueOf(localFieldVal).IsZero() {
			localMethod := localVal.MethodByName(remoteType.Field(i).Name)
			if localMethod.IsValid() {
				localMethod.Call([]reflect.Value{remotePtr})
			}
		} else if localFieldVal.IsValid() {
			if remoteFieldVal.IsValid() && remoteFieldVal.Type() == localFieldVal.Type() {
				remoteFieldVal.Set(localFieldVal)
			}
		}
	}

	return remoteVal.Interface().(node.Options)
}
