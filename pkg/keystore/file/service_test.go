package file_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/keystore/file"
	"github.com/gauss-project/aurorafs/pkg/keystore/test"
)

func TestService(t *testing.T) {
	dir, err := ioutil.TempDir("", "aurora-keystore-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	test.Service(t, file.New(dir))
}
