package wiredtiger

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

const (
	optionCacheSize     = "cache_size"
	optionCacheOverhead = "cache_overhead"
	optionCheckpoint    = "checkpoint"
	optionConfigBase    = "config_base"
	optionCreate        = "create"
	optionDebugMode     = "debug_mode"
	optionEviction      = "eviction"
	optionFileManager   = "file_manager"
	optionLog           = "log"
	optionSessionMax    = "session_max"
	optionStatistics    = "statistics"
	optionStatisticsLog = "statistics_log"
	optionExtensions    = "extensions"
	optionVerbose       = "verbose"
)

type Configuration map[string]string

const (
	optionSeparator = ','
	optionJoiner    = '='
)

func camelToSnake(s string) string {
	s = strings.TrimSpace(s)
	n := strings.Builder{}
	n.Grow(len(s) + 2) // nominal 2 bytes of extra space for inserted delimiters
	for i, v := range []byte(s) {
		vIsCap := v >= 'A' && v <= 'Z'
		vIsLow := v >= 'a' && v <= 'z'
		if vIsCap {
			v += 'a'
			v -= 'A'
		}

		// treat acronyms as words, eg for JSONData -> JSON is a whole word
		if i+1 < len(s) {
			next := s[i+1]
			vIsNum := v >= '0' && v <= '9'
			nextIsCap := next >= 'A' && next <= 'Z'
			nextIsLow := next >= 'a' && next <= 'z'
			nextIsNum := next >= '0' && next <= '9'
			// add underscore if next letter case type is changed
			if (vIsCap && (nextIsLow || nextIsNum)) || (vIsLow && (nextIsCap || nextIsNum)) || (vIsNum && (nextIsCap || nextIsLow)) {
				if vIsCap && nextIsLow {
					if prevIsCap := i > 0 && s[i-1] >= 'A' && s[i-1] <= 'Z'; prevIsCap {
						n.WriteByte('_')
					}
				}
				n.WriteByte(v)
				if vIsLow || vIsNum || nextIsNum {
					n.WriteByte('_')
				}
				continue
			}
		}

		if v == ' ' || v == '_' || v == '-' || v == '.' {
			// replace space/underscore/hyphen/dot with delimiter
			n.WriteByte('_')
		} else {
			n.WriteByte(v)
		}
	}

	return n.String()
}

func structToList(v interface{}, embed bool) string {
	rt := reflect.TypeOf(v)
	if rt.Kind() != reflect.Struct {
		panic(fmt.Errorf("need a struct to parse, given a %s", rt.Kind()))
	}

	rv := reflect.ValueOf(v)
	rs := reflect.TypeOf("")

	var res []string

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		ft := rv.Field(i)
		var value string
		switch ft.Kind() {
		case reflect.Bool:
			value = strconv.FormatBool(ft.Bool())
		case reflect.String:
			value = ft.String()
		case reflect.Struct:
			if stringer, ok := ft.Interface().(fmt.Stringer); ok {
				value = stringer.String()
			} else {
				value = structToList(ft.Interface(), true)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value = strconv.FormatInt(ft.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value = strconv.FormatUint(ft.Uint(), 10)
		case reflect.Float32:
			value = strconv.FormatFloat(ft.Float(), 'E', -1, 32)
		case reflect.Float64:
			value = strconv.FormatFloat(ft.Float(), 'E', -1, 64)
		default:
			if ft.CanInterface() {
				if stringer, ok := ft.Interface().(fmt.Stringer); ok {
					value = stringer.String()
				} else {
					panic(fmt.Errorf("field %s not implement fmt.Stringer", f.Name))
				}
			} else {
				if !ft.CanConvert(rs) {
					panic(fmt.Errorf("field %s not convert to string, is %s", f.Name, f.Type))
				}
				value = ft.Convert(rs).String()
			}
		}
		key := f.Tag.Get("key")
		if key == "" {
			key = camelToSnake(f.Name)
		}
		if key == "-" {
			if !ft.IsZero() {
				res = append(res, camelToSnake(f.Name))
			}
			continue
		}
		res = append(res, key+string(optionJoiner)+value)
	}

	if embed {
		return "(" + strings.Join(res, string(optionSeparator)) + ")"
	}

	return strings.Join(res, string(optionSeparator))
}

func (c *Configuration) Options(opts ...driver.Option) map[string]struct{} {
	cnf := make(map[string]string, len(opts))
	exported := make(map[string]struct{})

	for _, opt := range opts {
		var value string

		switch opt.Identity() {
		case optionCacheSize:
			value = opt.Value().(DiskSize).String()
		case optionCacheOverhead:
			value = strconv.Itoa(opt.Value().(int))
		case optionConfigBase:
			value = strconv.FormatBool(opt.Value().(bool))
		case optionCheckpoint:
			value = structToList(opt.Value().(Checkpoint), true)
		case optionCreate:
			value = strconv.FormatBool(opt.Value().(bool))
		case optionDebugMode:
			value = structToList(opt.Value().(DebugMode), true)
		case optionEviction:
			value = structToList(opt.Value().(Eviction), true)
		case optionFileManager:
			value = structToList(opt.Value().(FileManger), true)
		case optionLog:
			value = structToList(opt.Value().(Log), true)
		case optionSessionMax:
			value = strconv.Itoa(opt.Value().(int))
		case optionStatistics:
			var r string
			for _, policy := range opt.Value().([]StatisticsPolicy) {
				r += string(policy) + ","
			}
			value = "(" + strings.TrimRight(r, ",") + ")"
		case optionStatisticsLog:
			value = structToList(opt.Value().(StatisticsLog), true)
		case optionExtensions:
			value = opt.Value().(string)
		case optionVerbose:
			value = opt.Value().(string)
		default:
			panic("unsupported option:" + opt.Identity())
		}

		if opt.Exported() {
			exported[opt.Identity()] = struct{}{}
		}

		cnf[opt.Identity()] = value
	}

	*c = cnf

	return exported
}

type ByteType int

const (
	MB ByteType = iota
	GB
	TB
)

type DiskSize struct {
	Size int
	Type ByteType
}

func (ds DiskSize) String() string {
	var s string

	switch ds.Type {
	case MB:
		s = fmt.Sprintf("%dMB", ds.Size)
	case GB:
		s = fmt.Sprintf("%dGB", ds.Size)
	case TB:
		s = fmt.Sprintf("%dTB", ds.Size)
	default:
		s = "0"
	}

	return s
}

func (c *Configuration) SetCacheSize(n DiskSize) driver.Option {
	o := driver.NewOption(optionCacheSize, DiskSize{}, true)
	if (n.Type == MB && n.Size < 1) || (n.Type == TB && n.Size > 10) {
		n = DiskSize{Size: 100, Type: MB}
	}
	o.Set(n)
	return o
}

func (c *Configuration) SetCacheOverhead(n int) driver.Option {
	o := driver.NewOption(optionCacheOverhead, int(0), false)
	if n < 0 || n > 30 {
		n = 8
	}
	o.Set(n)
	return o
}

func (c *Configuration) SetCreate(b bool) driver.Option {
	o := driver.NewOption(optionCreate, false, false)
	o.Set(b)
	return o
}

type Checkpoint struct {
	LogSize DiskSize
	Wait    int
}

func (c *Configuration) SetCheckpoint(cp Checkpoint) driver.Option {
	o := driver.NewOption(optionCheckpoint, Checkpoint{}, false)
	if cp.LogSize.Size < 0 || (cp.LogSize.Type == GB && cp.LogSize.Size > 2) || cp.LogSize.Type == TB {
		cp.LogSize = DiskSize{Size: 0}
	}
	o.Set(cp)
	return o
}

func (c *Configuration) SetConfigBase(b bool) driver.Option {
	o := driver.NewOption(optionConfigBase, false, false)
	o.Set(b)
	return o
}

type DebugMode struct {
	CheckpointRetention int
	CursorCopy          bool
	Eviction            bool
	TableLogging        bool
}

func (c *Configuration) SetDebugMode(m DebugMode) driver.Option {
	o := driver.NewOption(optionDebugMode, DebugMode{}, false)
	if m.CheckpointRetention < 0 || m.CheckpointRetention > 1024 {
		m.CheckpointRetention = 0
	}
	o.Set(m)
	return o
}

type Eviction struct {
	ThreadsMax int `key:"threads_max"`
	ThreadsMin int `key:"threads_min"`
}

func (c *Configuration) SetEviction(e Eviction) driver.Option {
	o := driver.NewOption(optionEviction, Eviction{}, false)
	if e.ThreadsMax < 1 || e.ThreadsMax > 20 {
		e.ThreadsMax = 8
	}
	if e.ThreadsMin < 1 || e.ThreadsMin > 20 {
		e.ThreadsMin = 1
	}
	if e.ThreadsMax < e.ThreadsMin {
		e.ThreadsMin, e.ThreadsMax = e.ThreadsMax, e.ThreadsMin
	}
	o.Set(e)
	return o
}

type FileManger struct {
	CloseHandleMinimum int
	CloseIdleTime      int
	CloseScanInterval  int
}

func (c *Configuration) SetFileManager(f FileManger) driver.Option {
	o := driver.NewOption(optionFileManager, FileManger{}, false)
	if f.CloseHandleMinimum < 0 {
		f.CloseHandleMinimum = 250
	}
	if f.CloseIdleTime < 0 || f.CloseIdleTime > 100000 {
		f.CloseIdleTime = 30
	}
	if f.CloseScanInterval < 0 || f.CloseScanInterval > 100000 {
		f.CloseScanInterval = 10
	}
	o.Set(f)
	return o
}

type Log struct {
	Archive    bool
	Compressor Compressor
	Enabled    bool
	Path       string
}

type Compressor string

const (
	NoneCompressor   Compressor = "none"
	LZ4Compressor    Compressor = "lz4"
	SnappyCompressor Compressor = "snappy"
	ZLibCompressor   Compressor = "zlib"
	ZstdCompressor   Compressor = "zstd"
)

func (c *Configuration) SetLog(l Log) driver.Option {
	o := driver.NewOption(optionLog, Log{}, false)
	if strings.HasPrefix(l.Path, string(os.PathSeparator)) {
		panic("log path should be a relative path under database home")
	}
	o.Set(l)
	return o
}

func (c *Configuration) SetSessionMax(n int) driver.Option {
	o := driver.NewOption(optionSessionMax, int(0), false)
	if n < 1 {
		n = 100
	}
	o.Set(n)
	return o
}

type StatisticsPolicy string

const (
	StatisticsAll       StatisticsPolicy = "all"
	StatisticsCacheWalk StatisticsPolicy = "cache_walk"
	StatisticsFast      StatisticsPolicy = "fast"
)

func (c *Configuration) SetStatistics(s []StatisticsPolicy) driver.Option {
	o := driver.NewOption(optionStatistics, []StatisticsPolicy{StatisticsAll}, false)
	o.Set(s)
	return o
}

type StatisticsLog struct {
	Wait int `key:"wait"`
}

func (c *Configuration) SetStatisticsLog(l StatisticsLog) driver.Option {
	o := driver.NewOption(optionStatisticsLog, StatisticsLog{}, false)
	if l.Wait < 0 || l.Wait > 100000 {
		l.Wait = 0
	}
	o.Set(l)
	return o
}

func (c *Configuration) SetExtensions(s string) driver.Option {
	o := driver.NewOption(optionExtensions, "", false)
	if len(s) != 0 {
		o.Set(s)
	}
	return o
}

func (c *Configuration) SetVerbose(s string) driver.Option {
	o := driver.NewOption(optionVerbose, "", false)
	if len(s) != 0 {
		o.Set(s)
	}
	return o
}

func (c Configuration) String() string {
	var opts []string

	for k, v := range c {
		opts = append(opts, k+string(optionJoiner)+v)
	}

	return strings.Join(opts, string(optionSeparator))
}
