package cmd_test

import (
	"bytes"
	"testing"

	"github.com/gauss-project/aurorafs"
	"github.com/gauss-project/aurorafs/cmd/aurorafs/cmd"
)

func TestVersionCmd(t *testing.T) {
	var outputBuf bytes.Buffer
	if err := newCommand(t,
		cmd.WithArgs("version"),
		cmd.WithOutput(&outputBuf),
	).Execute(); err != nil {
		t.Fatal(err)
	}

	want := bee.Version + "\n"
	got := outputBuf.String()
	if got != want {
		t.Errorf("got output %q, want %q", got, want)
	}
}
