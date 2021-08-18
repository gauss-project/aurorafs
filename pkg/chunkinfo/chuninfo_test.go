package chunkinfo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"testing"
)

const fileContentType = "text/plain; charset=utf-8"

var (
	debug      = false
	simpleData = []byte("hello test world") // fixed, 16 bytes
)

func TestFindChunkInfo(t *testing.T) {
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")
	cid := boson.MustParseHexAddress("03")
	rootCid, s := mockUploadFile(t)
	server1 := mockChunkInfo(t, s, nil)
	server1.newQueue(rootCid.String())
	recorder1 := streamtest.New(
		streamtest.WithBaseAddr(clientAddress),
		streamtest.WithProtocols(server1.Protocol()),
	)
	server := mockChunkInfo(t, s, recorder1)
	server.OnChunkTransferred(cid, rootCid, serverAddress)
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(clientAddress),
	)
	client := mockChunkInfo(t, s, recorder)
	client.FindChunkInfo(context.Background(), nil, rootCid, []boson.Address{serverAddress})

	records, err := recorder.Records(serverAddress, "chunkinfo", "1.0.0", "chunkpyramid/req")
	if err != nil {
		t.Fatal(err)
	}
	if l := len(records); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	record := records[0]
	// validate received ping greetings from the client
	messages, err := protobuf.ReadMessages(
		bytes.NewReader(record.In()),
		func() protobuf.Message { return new(pb.ChunkPyramidReq) },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(messages)
	records1, err1 := recorder1.Records(clientAddress, "chunkinfo", "1.0.0", "chunkpyramid/resp")
	if err1 != nil {
		t.Fatal(err)
	}
	if l := len(records1); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	record1 := records1[0]
	messages1, err := protobuf.ReadMessages(
		bytes.NewReader(record1.In()),
		func() protobuf.Message { return new(pb.ChunkPyramidResp) },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(messages1)
	v := server1.GetChunkInfo(rootCid, cid)
	t.Logf("%v", v)
}

func TestHandlerChunkInfoReq(t *testing.T) {

}

func TestHandlerChunkInfoResp(t *testing.T) {

}
func TestHandlerPyramidReq(t *testing.T) {

}
func TestHandlerPyramidResp(t *testing.T) {

}

func TestQueueProcess(t *testing.T) {

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
	addr := boson.MustParseHexAddress("ca1e9f3938cc1425c6061b96ad9eb93e134dfe8734ad490164ef20af9d1cf59c")
	rc := rootCid.String()
	a := mockChunkInfo(t, s, recorder)
	a.cpd.updatePendingFinder(rootCid)
	a.newQueue(rc)
	a.getQueue(rc).push(Pulling, addr.Bytes())
	// max pulling
	for i := 0; i < PullMax; i++ {
		overlay := test.RandomAddress()
		if i == 0 {
			a.updateQueue(context.Background(), nil, rootCid, addr, [][]byte{overlay.Bytes()})
		} else {
			a.updateQueue(context.Background(), nil, rootCid, boson.NewAddress(*a.getQueue(rc).pop(Pulling)), [][]byte{overlay.Bytes()})
		}
	}
	if len(a.getQueue(rc).getPull(Pulled)) != PullMax {
		t.Fatalf("pulled len error")
	}

	if len(a.getQueue(rc).getPull(UnPull)) != 1 {
		t.Fatalf("unpull len error")
	}

}

func newTestProtocol(h p2p.HandlerFunc, protocolName, protocolVersion, streamName string) p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: h,
			},
		},
	}
}
func mockUploadFile(t *testing.T) (boson.Address, traversal.Service) {
	chunkCount := 10 + 1

	ctx := context.Background()
	largeBytesData := generateSampleData(boson.ChunkSize * chunkCount)

	mockStoreA := mock.NewStorer()
	traversalService := traversal.NewService(mockStoreA)

	reference, _ := uploadFile(t, ctx, mockStoreA, largeBytesData, "", false)
	return reference, traversalService
}

func mockChunkInfo(t *testing.T, traversal traversal.Service, r *streamtest.Recorder) *ChunkInfo {
	logger := logging.New(ioutil.Discard, 0)
	server := New(r, logger, traversal)
	return server
}

func generateSampleData(size int) (b []byte) {
	for {
		b = append(b, simpleData...)
		if len(b) >= size {
			break
		}
	}

	b = b[:size]

	return b
}

func uploadFile(t *testing.T, ctx context.Context, store storage.Storer, file []byte, filename string, encrypted bool) (boson.Address, string) {
	pipe := builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(file), int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}

	if debug {
		verifyChunkHash(t, ctx, store, fr)
	}

	if filename == "" {
		filename = fr.String()
	}

	m := entry.NewMetadata(filename)
	m.MimeType = fileContentType
	metadata, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	pipe = builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	mr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(metadata), int64(len(metadata)))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("metadata hash=%s\n", mr)

	entries := entry.New(fr, mr)
	entryBytes, err := entries.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	pipe = builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	reference, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(entryBytes), int64(len(entryBytes)))
	if err != nil {
		t.Fatal(reference)
	}
	t.Logf("reference hash=%s\n", reference)

	return reference, filename
}

func verifyChunkHash(t *testing.T, ctx context.Context, store storage.Storer, rootHash boson.Address) {
	val, err := store.Get(ctx, storage.ModeGetRequest, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	chunkData := val.Data()
	t.Logf("get val data size=%d\n", len(chunkData))
	if len(chunkData) > 16 && len(chunkData) <= boson.ChunkSize+8 {
		trySpan := chunkData[:8]
		if size := binary.LittleEndian.Uint64(trySpan); size > uint64(len(chunkData[8:])) {
			for i := uint64(8); i < uint64(len(chunkData)-8); i += 32 {
				t.Logf("bmt root hash is %s\n", hex.EncodeToString(chunkData[i:i+32]))
			}
		} else {
			t.Logf("bmt root hash is %s, span = %d\n", rootHash.String(), size)
		}
	}
}
