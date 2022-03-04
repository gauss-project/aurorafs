package wiredtiger

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lwiredtiger

#include <string.h>
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

int wiredtiger_set_timestamp(WT_CONNECTION *connection, const char *config) {
	return connection->set_timestamp(connection, config);
}

int wiredtiger_query_timestamp(WT_CONNECTION *connection, char * hex_timestamp, const char *config) {
	return connection->query_timestamp(connection, hex_timestamp, config);
}
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the shed/driver package.
type Driver struct{}

var (
	defaultCreate        = true
	defaultCacheSize     = DiskSize{Size: 2, Type: GB}
	defaultCacheOverhead = 8
	defaultCheckpoint    = Checkpoint{LogSize: DiskSize{Size: 2, Type: GB}, Wait: 60}
	defaultConfigBase    = false
	defaultDebugMode     = DebugMode{CheckpointRetention: 0, CursorCopy: false, Eviction: false, TableLogging: false}
	defaultEviction      = Eviction{ThreadsMin: 4, ThreadsMax: 4}
	defaultFileManager   = FileManger{CloseIdleTime: 600, CloseScanInterval: 10, CloseHandleMinimum: 2000}
	defaultLog           = Log{Enabled: true, Archive: true, Path: "journal", Compressor: SnappyCompressor}
	defaultSessionMax    = 33000
	defaultStatistics    = []StatisticsPolicy{StatisticsFast}
	defaultStatisticsLog = StatisticsLog{Wait: 0}
	defaultExtensions    = "[/usr/local/lib/libwiredtiger_snappy.so]"
	defaultVerbose       = "[recovery_progress,checkpoint_progress,compact_progress]"
)

func checkDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// TODO change dir perm
			err = os.Mkdir(path, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	_ = f.Close()

	return nil
}

func (d Driver) Open(path, options string) (driver.DB, error) {
	var (
		config Configuration
		result int
		parser *C.WT_CONFIG_PARSER
		conn   *C.WT_CONNECTION
	)

	exported := config.Options(
		config.SetCreate(defaultCreate),
		config.SetCacheSize(defaultCacheSize),
		config.SetCacheOverhead(defaultCacheOverhead),
		config.SetCheckpoint(defaultCheckpoint),
		config.SetConfigBase(defaultConfigBase),
		config.SetDebugMode(defaultDebugMode),
		config.SetEviction(defaultEviction),
		config.SetFileManager(defaultFileManager),
		config.SetLog(defaultLog),
		config.SetSessionMax(defaultSessionMax),
		config.SetStatistics(defaultStatistics),
		config.SetStatisticsLog(defaultStatisticsLog),
		config.SetExtensions(defaultExtensions),
		config.SetVerbose(defaultVerbose),
	)

	// override user config
	if options != "" {
		var override map[string]string
		err := json.Unmarshal([]byte(options), &override)
		if err != nil {
			return nil, err
		}

		for k, v := range override {
			if _, ok := exported[k]; ok {
				config[k] = v
			}
		}
	}

	if err := checkDirectory(path); err != nil {
		return nil, err
	}

	// create journal dir
	if err := checkDirectory(filepath.Join(path, "journal")); err != nil {
		return nil, err
	}

	pathStr := C.CString(path)
	configStr := C.CString(config.String())

	defer C.free(unsafe.Pointer(pathStr))
	defer C.free(unsafe.Pointer(configStr))

	result = int(C.wiredtiger_config_parser_open(nil, configStr, C.size_t(C.strlen(configStr)), &parser))
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

	fmt.Println(config)

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
		config:  config,
		pool:    pool,
		closing: make(chan struct{}),
	}

	var ts *C.char = nil

	ts = (*C.char)(C.malloc(C.size_t(17)))
	tsConfig := C.CString("get=recovery")

	defer C.free(unsafe.Pointer(tsConfig))
	defer C.free(unsafe.Pointer(ts))

	result = int(C.wiredtiger_query_timestamp(conn, ts, tsConfig))
	if checkError(result) {
		return nil, NewError(result)
	}

	tsStr := C.GoString(ts)
	logger.Println("recovery_timestamp=", tsStr)

	if tsStr > "0" {
		err = setTimestamp(conn, tsStr, false)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func setTimestamp(conn *C.WT_CONNECTION, ts string, stable bool) error {
	var configStr *C.char = nil

	if stable {
		configStr = C.CString(fmt.Sprintf("stable_timestamp=%s", ts))
	} else {
		configStr = C.CString(fmt.Sprintf("oldest_timestamp=%s,durable_timestamp=%s", ts, ts))
	}

	defer C.free(unsafe.Pointer(configStr))

	result := int(C.wiredtiger_set_timestamp(conn, configStr))
	if checkError(result) {
		return NewError(result)
	}

	return nil
}
