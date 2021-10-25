package file_test

import (
	"os"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/keystore/file"
	"github.com/gauss-project/aurorafs/pkg/keystore/test"
)

func TestService(t *testing.T) {
	dir, err := os.MkdirTemp("", "aurora-keystore-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	test.Service(t, file.New(dir))
}
