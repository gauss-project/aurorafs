package chunkinfo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	smock "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

const fileContentType = "text/plain; charset=utf-8"

var (
	debug      = false
	simpleData = []byte("hello test world") // fixed, 16 bytes
)

func addBody() io.Reader {
	return body(`{"rootcid": "6aa47f0d31e20784005cb2148b6fed85e538f829698ef552bb590be1bfa7e643", "address": "ca1e9f3938cc1425c6061b96ad9eb93e134dfe8734ad490164ef20af9d1cf59c"}`)
}

func body(in string) io.Reader {
	return strings.NewReader(in)
}

func TestInit(t *testing.T) {

	serverAddress := boson.MustParseHexAddress("01")
	rootCid, _ := boson.ParseHexAddress("6aa47f0d31e20784005cb2148b6fed85e538f829698ef552bb590be1bfa7e643")

	_, s := mockUploadFile(t)
	recorder := streamtest.New(
		streamtest.WithBaseAddr(serverAddress),
	)

	server := mockChunkInfo(s, recorder)
	if _, err := http.Post(fmt.Sprintf("http://%s/api/v1.0/rcid", server.oracleUrl), "application/json", addBody()); err != nil {
		t.Fatal("oracle link error")
	}

	if server.Init(context.Background(), nil, rootCid) {
		t.Fatalf(" want false")
	}
}

func TestHandlerChunkInfoReq(t *testing.T) {
	clientAddress := boson.MustParseHexAddress("01")
	serverAddress := boson.MustParseHexAddress("02")
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
		streamtest.WithBaseAddr(clientAddress),
	)

	a := mockChunkInfo(s, recorder)
	a.newQueue(rootCid.String())
	a.getQueue(rootCid.String()).push(UnPull, serverAddress.Bytes())
	a.cpd.updatePendingFinder(rootCid)
	ctx := context.Background()
	a.doFindChunkInfo(ctx, nil, rootCid)

	reqRecords, err := recorder.Records(serverAddress, "chunkinfo", "1.0.0", "chunkinforeq")

	if err != nil {
		t.Fatal(err)
	}
	if l := len(reqRecords); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	respRecord := reqRecords[0]
	reqMessages, err := protobuf.ReadMessages(
		bytes.NewReader(respRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoReq) },
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(reqMessages)
}

func TestHandlerChunkInfoResp(t *testing.T) {
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")

	rootCid, s := mockUploadFile(t)
	server1 := mockChunkInfo(s, nil)
	server1.newQueue(rootCid.String())
	recorder1 := streamtest.New(
		streamtest.WithBaseAddr(serverAddress),
		streamtest.WithProtocols(server1.Protocol()),
	)
	b := mockChunkInfo(s, recorder1)

	recorder := streamtest.New(
		streamtest.WithProtocols(b.Protocol()),
		streamtest.WithBaseAddr(clientAddress),
	)
	ctx := context.Background()
	b.newQueue(rootCid.String())
	b.getQueue(rootCid.String()).push(Pulling, serverAddress.Bytes())
	b.cpd.updatePendingFinder(rootCid)
	tree, _ := b.getChunkPyramid(ctx, rootCid)
	pram, _ := b.traversal.CheckTrieData(ctx, rootCid, tree)
	if err := b.initChunkInfoTabNeighbor(); err != nil {
		t.Fatal(err)
	}
	if err := b.OnChunkTransferred(boson.NewAddress(pram[0][0]), rootCid, clientAddress); err != nil {
		t.Fatal(err)
	}
	var vb bitVector
	b.storer.Get(generateKey(keyPrefix, rootCid, clientAddress), &vb)
	vf, err := bitvector.NewFromBytes(vb.B, vb.Len)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(vf.String())
	a := mockChunkInfo(s, recorder)

	req := a.cd.createChunkInfoReq(rootCid)

	if err := b.onChunkInfoReq(ctx, nil, clientAddress, req); err != nil {
		t.Fatal(err)
	}

	respRecords, err := recorder1.Records(clientAddress, "chunkinfo", "1.0.0", "chunkinforesp")
	if err != nil {
		t.Fatal(err)
	}

	if l := len(respRecords); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	respRecord := respRecords[0]
	respMessages, err := protobuf.ReadMessages(
		bytes.NewReader(respRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoResp) },
	)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(respMessages)
	v := server1.GetChunkInfo(rootCid, boson.NewAddress(pram[0][0]))
	if v == nil || len(v) == 0 {
		t.Fatalf("bit vector error")
	}
}

func TestHandlerPyramid(t *testing.T) {
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")
	cid := boson.MustParseHexAddress("03")
	rootCid, s := mockUploadFile(t)
	server := mockChunkInfo(s, nil)
	server.OnChunkTransferred(cid, rootCid, serverAddress)
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(clientAddress),
	)
	client := mockChunkInfo(s, recorder)
	err := client.doFindChunkPyramid(context.Background(), nil, rootCid, serverAddress)
	if err != nil {
		t.Fatal(err)
	}

	records, err := recorder.Records(serverAddress, "chunkinfo", "1.0.0", "chunkpyramidhash")
	if err != nil {
		t.Fatal(err)
	}
	if l := len(records); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	record := records[0]
	messages, err := protobuf.ReadMessages(
		bytes.NewReader(record.In()),
		func() protobuf.Message { return new(pb.ChunkPyramidHashReq) },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(messages)
	if err != nil {
		t.Fatal(err)
	}
	cids := client.cp.getChunkCid(rootCid)
	t.Log(cids)
	if cids == nil || len(cids) == 0 {
		t.Fatalf("chunk pyramid is nil")
	}
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
	aAddress := mockChunkInfo(s, recorder)
	aAddress.cpd.updatePendingFinder(rootCid)
	aAddress.newQueue(rc)
	aAddress.getQueue(rc).push(Pulling, addr.Bytes())
	// max pulling
	for i := 0; i < PullMax; i++ {
		overlay := test.RandomAddress()
		if i == 0 {
			aAddress.updateQueue(context.Background(), nil, rootCid, addr, [][]byte{overlay.Bytes()})
		} else {
			if aAddress.getQueue(rc).len(Pulled) == 168 {
				fmt.Println("")
			}
			aAddress.updateQueue(context.Background(), nil, rootCid, boson.NewAddress(*aAddress.getQueue(rc).pop(Pulling)), [][]byte{overlay.Bytes(), test.RandomAddress().Bytes(), test.RandomAddress().Bytes(), test.RandomAddress().Bytes(), test.RandomAddress().Bytes(), test.RandomAddress().Bytes(), test.RandomAddress().Bytes()})
		}
	}
	if len(aAddress.getQueue(rc).getPull(Pulled)) != PullMax {
		t.Fatalf("pulled len error")
	}

	if len(aAddress.getQueue(rc).getPull(UnPull)) != 1000 {
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

func mockChunkInfo(traversal traversal.Service, r *streamtest.Recorder) *ChunkInfo {
	logger := logging.New(ioutil.Discard, 0)
	ret := smock.NewStateStore()
	server := New(r, logger, traversal, ret, "127.0.0.1:8000")
	server.InitChunkInfo()
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
