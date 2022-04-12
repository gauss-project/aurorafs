package netrelay

import (
	"strings"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/aurora"
)

func TestService_RelayHttpDo(t *testing.T) {
	url := strings.ReplaceAll(aurora.RelayPrefixHttp+"/test1/test2", aurora.RelayPrefixHttp, "")
	urls := strings.Split(url, "/")
	group := urls[1]
	if group != "test1" {
		t.Fatal("url parse err")
	}
}
