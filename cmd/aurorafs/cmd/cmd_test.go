package cmd_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/gauss-project/aurorafs/cmd/aurorafs/cmd"
)

var homeDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "aurorafs-cmd-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	homeDir = dir

	code := m.Run()
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func newCommand(t *testing.T, opts ...cmd.Option) (c *cmd.Command) {
	t.Helper()

	c, err := cmd.NewCommand(append([]cmd.Option{cmd.WithHomeDir(homeDir)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
