package chunkinfo

import (
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"io/ioutil"
	"testing"
)

func TestChunkPyramid(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)

	server := New(nil, logger)
	addr := boson.MustParseHexAddress("126140bb0a33d62c4efb0523db2c26be849fcf458504618de785e2a219bad374")
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
	)
	rootCid := boson.NewAddress([]byte("rootCid"))
	cid := boson.NewAddress([]byte("cid"))
	server.OnChunkTransferred(cid, rootCid, addr)
	client := New(recorder, logger)
	client.FindChunkInfo(context.Background(), nil, rootCid, []boson.Address{addr})
	for s := range server.cp.pyramid {
		fmt.Printf(s)
	}
	for s := range client.cp.pyramid {
		fmt.Printf(s)
	}
}
