package driver

import (
	"fmt"
	"reflect"
)

type Option interface {
	Identity() string
	Value() interface{}
	Exported() bool
}

type Configure interface {
	Options(opts ...Option) (exported map[string]struct{})
}

type OptionValue struct {
	key    string
	typ    reflect.Type
	value  interface{}
	export bool
}

func NewOption(key string, initial interface{}, export bool) *OptionValue {
	return &OptionValue{
		key:    key,
		typ:    reflect.TypeOf(initial),
		export: export,
	}
}

func (ov *OptionValue) Set(v interface{}) {
	rv := reflect.ValueOf(v)

	if rv.Kind() == reflect.Ptr {
		ov.Set(rv.Elem().Interface())

		return
	}

	if rv.Kind() != ov.typ.Kind() {
		panic(fmt.Errorf("%s want type %s, got a %s", ov.key, ov.typ.Name(), rv.Kind()))
	}

	if ov.typ.Kind() == reflect.Slice {
		if rv.Index(0).Kind() != ov.typ.Elem().Kind() {
			panic(fmt.Errorf("%s want type []%s, got a []%s", ov.key, ov.typ.Name(), rv.Kind()))
		}
	}

	switch ov.typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fallthrough
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		ov.value = v
	case reflect.Uint64:
		u64 := rv.Uint()
		if u64 >= 1<<63 {
			panic(fmt.Errorf("uint64 values with high bit set are not supported"))
		}
		ov.value = v
	case reflect.Float32, reflect.Float64:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.String:
		ov.value = v
	case reflect.Struct:
		if !rv.Type().AssignableTo(ov.typ) {
			panic(fmt.Errorf("unsupported type %T, a %s", v, rv.Kind()))
		}
		ov.value = v
	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek == reflect.Complex64 || ek == reflect.Complex128 || ek == reflect.Interface ||
			ek == reflect.Slice || ek == reflect.Array || ek == reflect.Map ||
			ek == reflect.Uintptr || ek == reflect.UnsafePointer {
			panic(fmt.Errorf("unsupported type %T, a slice of %s", v, ek))
		}
		ov.value = v
	}
}

func (ov *OptionValue) Identity() string {
	return ov.key
}

func (ov *OptionValue) Value() interface{} {
	return ov.value
}

func (ov *OptionValue) Exported() bool {
	return ov.export
}
