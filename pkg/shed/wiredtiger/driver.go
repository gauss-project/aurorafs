package wiredtiger

/*
#cgo LDFLAGS: -lwiredtiger
#include <stdlib.h>
#include <wiredtiger.h>

int64_t wiredtiger_config_item_value(WT_CONFIG_ITEM *item) {
	return item->val;
}

int wiredtiger_parser_get(WT_CONFIG_PARSER *parser, const char *key, WT_CONFIG_ITEM *value) {
	return parser->get(parser, key, value);
}

int wiredtiger_config_parser_close(WT_CONFIG_PARSER *parser) {
	return parser->close(parser);
}
*/
import "C"
import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the shed/driver package.
type Driver struct{}

var (
	defaultCreate        = true
	defaultCacheSize     = uint64(2 * 1024 * 1024 * 1024)
	defaultCacheOverhead = 8
	defaultConfigBase    = false
	defaultDebugMode     = DebugMode{CheckPointRetention: 0, CursorCopy: false, Eviction: false, TableLogging: false}
	defaultEviction      = Eviction{ThreadsMin: 4, ThreadsMax: 4}
	defaultFileManager   = FileManger{CloseIdleTime: 100000, CloseScanInterval: 10, CloseHandleMinimum: 250}
	defaultLog           = Log{Enabled: true, Archive: true, Path: "journal", Compressor: NoneCompressor}
	defaultSessionMax    = 33000
	defaultStatistics    = []StatisticsPolicy{StatisticsFast}
	defaultStatisticsLog = StatisticsLog{Wait: 0}
	defaultVerbose       = "[recovery_progress,checkpoint_progress,compact_progress]"
)

func (d Driver) Init() driver.Configure {
	var c Configuration

	c.Options(
		c.SetCreate(defaultCreate),
		c.SetCacheSize(defaultCacheSize),
		c.SetCacheOverhead(defaultCacheOverhead),
		c.SetConfigBase(defaultConfigBase),
		c.SetDebugMode(defaultDebugMode),
		c.SetEviction(defaultEviction),
		c.SetFileManager(defaultFileManager),
		c.SetLog(defaultLog),
		c.SetSessionMax(defaultSessionMax),
		c.SetStatistics(defaultStatistics),
		c.SetStatisticsLog(defaultStatisticsLog),
		c.SetVerbose(defaultVerbose),
	)

	return &c
}

func (d Driver) Open(path string) (driver.DB, error) {
	c := d.Init().(*Configuration)

	var (
		result int
		parser *C.WT_CONFIG_PARSER
		conn   *C.WT_CONNECTION
	)

	_, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// TODO change dir perm
			err = os.Mkdir(path, os.ModePerm)
			if err != nil {
				return nil, err
			}

			// create journal dir
			err = os.Mkdir(path+string(os.PathSeparator)+"journal", os.ModePerm)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	pathStr := C.CString(path)
	configStr := C.CString(string(*c))

	defer C.free(unsafe.Pointer(pathStr))
	defer C.free(unsafe.Pointer(configStr))

	result = int(C.wiredtiger_config_parser_open(nil, configStr, C.size_t(len(string(*c))), &parser))
	if checkError(result) {
		return nil, NewError(result)
	}

	var item C.WT_CONFIG_ITEM
	var keyStr *C.char = C.CString(optionSessionMax)

	result = int(C.wiredtiger_parser_get(parser, keyStr, &item))
	if checkError(result) {
		return nil, NewError(result)
	}

	value := uint64(C.wiredtiger_config_item_value(&item))
	if value > 0 {
		sessionMaxSize = value
	}

	result = int(C.wiredtiger_config_parser_close(parser))
	if checkError(result) {
		return nil, NewError(result)
	}

	fmt.Println(*c)

	result = int(C.wiredtiger_open(pathStr, nil, configStr, &conn))
	if checkError(result) {
		return nil, NewError(result)
	}

	poolSize := runtime.NumCPU()
	pool, err := newSessionPool(conn, uint64(poolSize))
	if err != nil {
		return nil, err
	}

	db := &DB{
		conn:    conn,
		config:  c,
		pool:    pool,
		closing: make(chan struct{}),
	}

	err = db.initSchema()
	if err != nil {
		return nil, err
	}

	return db, nil
}
