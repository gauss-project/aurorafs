package chunkinfo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
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
}

func TestHandlerChunkInfoReq(t *testing.T) {
	findAddress := boson.MustParseHexAddress("03")
	rootCid, s := mockUploadFile(t)

	client := mockChunkInfo(t, s, nil)
	client.newQueue(rootCid.ByteString())
	var bosoary []boson.Address
	bosoary = append(bosoary, findAddress)
	client.getQueue(rootCid.ByteString()).push(Pulling, bosoary[0].Bytes())

	q := client.getQueue(rootCid.ByteString())
	if q == nil {
		t.Fatal("getQueue Error")
	}

	unNode := q.pop(UnPull)
	q.push(Pulling, *unNode)
	client.tt.updateTimeOutTrigger(rootCid.Bytes(), *unNode)
	ciReq := client.cd.createChunkInfoReq(rootCid)
	ctx := context.Background()
	client.sendData(ctx, boson.NewAddress(*unNode), streamChunkInfoReqName, ciReq)
}

func TestHandlerChunkInfoResp(t *testing.T) {
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")
	rootCid, s := mockUploadFile(t)
	server1 := mockChunkInfo(t, s, nil)
	recorder1 := streamtest.New(
		streamtest.WithBaseAddr(serverAddress),
		streamtest.WithProtocols(server1.Protocol()),
	)

	server := mockChunkInfo(t, s, recorder1)
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(clientAddress),
	)
	client := mockChunkInfo(t, s, recorder)
	client.FindChunkInfo(context.Background(), nil, rootCid, []boson.Address{serverAddress})
	server1.GetChunkInfo(serverAddress, clientAddress)
	fmt.Println("sfds")
}
func TestHandlerPyramidReq(t *testing.T) {

}
func TestHandlerPyramidResp(t *testing.T) {

}

func TestQueueProcess(t *testing.T) {

}

func TestQueueUpdate(t *testing.T) {

}

func mockUploadFile(t *testing.T) (boson.Address, traversal.Service) {
	chunkCount := 10 + 1

	ctx := context.Background()
	largeBytesData := generateSampleData(boson.BigChunkSize * chunkCount)

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
	if len(chunkData) > 16 && len(chunkData) <= boson.BigChunkSize+8 {
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
