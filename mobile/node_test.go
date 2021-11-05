package mobile_test

import (
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/mobile"
)

func TestNewNode(t *testing.T) {
	var (
		node *mobile.Node
		opts *mobile.Options
		err  error
	)

	opts, err = mobile.ExportDefaultConfig()
	if err != nil {
		t.Fatal("export config:", err)
	}

	node, err = mobile.NewNode(opts)
	if err != nil {
		t.Fatal("new node:", err)
	}

	time.Sleep(3 * time.Second)

	err = node.Stop()
	if err != nil {
		t.Fatal("close node:", err)
	}
}
