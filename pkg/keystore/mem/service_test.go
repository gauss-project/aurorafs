package mem_test

import (
	"testing"

	"github.com/gauss-project/aurorafs/pkg/keystore/mem"
	"github.com/gauss-project/aurorafs/pkg/keystore/test"
)

func TestService(t *testing.T) {
	test.Service(t, mem.New())
}
