package chunkinfo

import (
	"bufio"
	"context"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestChunkInfoSource(t *testing.T) {
	clientAddress := boson.MustParseHexAddress("01")
	cid := boson.MustParseHexAddress("02")
	rootCid, s := mockUploadFile(t)
	recorder := streamtest.New(
		streamtest.WithProtocols(
			newTestProtocol(func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
				if _, err := bufio.NewReader(stream).ReadString('\n'); err != nil {
					return err
				}
				var g errgroup.Group
				g.Go(stream.Close)
				g.Go(stream.FullClose)

				if err := g.Wait(); err != nil {
					return err
				}
				return stream.FullClose()
			}, protocolName, protocolVersion, streamChunkInfoReqName)),
	)

	a := mockChunkInfo(s, recorder, clientAddress)

	err := a.cs.updatePyramidSource(rootCid, clientAddress)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.UpdateChunkInfoSource(rootCid, clientAddress, cid); err != nil {
		t.Fatal(err)
	}

	source := a.GetChunkInfoSource(rootCid)
	if len(source.ChunkSource) == 0 {
		t.Fatal(" Get chunk source to zero.")
	}
	err = a.InitChunkInfo()
	if err != nil {
		t.Fatal("ChunkInfo Init Failed:", err.Error())
	}

	delfile := a.DelFile(rootCid, func() {})
	if !delfile {
		t.Fatal("delete file error")
	}

	source = a.GetChunkInfoSource(rootCid)
	if len(source.ChunkSource) != 0 || source.PyramidSource != "" {
		t.Fatal("Failed to delete source record.")
	}

	if err := a.UpdatePyramidSource(rootCid, clientAddress); err != nil {
		t.Fatal(err)
	}

	source = a.GetChunkInfoSource(rootCid)
	if source.PyramidSource != clientAddress.String() {
		t.Fatal("Pyramid values are unequal.")
	}

	delfile = a.DelFile(rootCid, func() {})
	if !delfile {
		t.Fatal("delete file error")
	}

	source = a.GetChunkInfoSource(rootCid)
	if len(source.ChunkSource) != 0 || source.PyramidSource != "" {
		t.Fatal("Failed to delete source record.")
	}

	fmt.Println("Test Success")

}
