package mobile_test

import (
	"reflect"
	"testing"

	"github.com/gauss-project/aurorafs/mobile"
)

func TestExport(t *testing.T) {
	opts, err := mobile.ExportDefaultConfig()
	if err != nil {
		t.Fatal(err)
	}

	nodeOpts := mobile.ExportOptions(opts)

	nodeOptsVal := reflect.ValueOf(&nodeOpts).Elem()
	nodeOptsType := reflect.TypeOf(&nodeOpts).Elem()
	for i := 0; i < nodeOptsVal.NumField(); i++ {
		t.Logf("field %s val %v", nodeOptsType.Field(i).Name, nodeOptsVal.Field(i))
	}
}
