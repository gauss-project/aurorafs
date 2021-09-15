package mock_test

import (
	"testing"

	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/statestore/test"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

func TestMockStateStore(t *testing.T) {
	test.Run(t, func(t *testing.T) storage.StateStorer {
		return mock.NewStateStore()
	})
}
