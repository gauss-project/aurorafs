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
	rmock "github.com/gauss-project/aurorafs/pkg/routetab/mock"
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
	"time"
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

	server := mockChunkInfo(s, recorder, serverAddress)
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
	)

	a := mockChunkInfo(s, recorder, clientAddress)
	a.newQueue(rootCid.String())
	a.getQueue(rootCid.String()).push(UnPull, serverAddress.Bytes())
	a.cpd.updatePendingFinder(rootCid)
	ctx := context.Background()
	a.doFindChunkInfo(ctx, nil, rootCid)
	time.Sleep(5 * time.Second)
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

func TestHandlerChunkInfoReqRelay(t *testing.T) {
	// req a->b->c
	cAddress := boson.MustParseHexAddress("03")
	bAddress := boson.MustParseHexAddress("02")
	aAddress := boson.MustParseHexAddress("01")

	rootCid, tra := mockUploadFile(t)

	recorder := streamtest.New(
		streamtest.WithBaseAddr(bAddress),
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
			}, protocolName, protocolVersion, streamChunkInfoRespName)),
	)

	cOverlay := mockChunkInfo(tra, recorder, cAddress)
	cRecorder := streamtest.New(streamtest.WithProtocols(cOverlay.Protocol()), streamtest.WithBaseAddr(bAddress))
	bOverlay := mockChunkInfo(tra, cRecorder, bAddress)
	bRecorder := streamtest.New(streamtest.WithProtocols(bOverlay.Protocol()), streamtest.WithBaseAddr(aAddress))
	aOverlay := mockChunkInfo(tra, bRecorder, aAddress)

	ctx := context.Background()
	aOverlay.findChunkInfo(ctx, nil, rootCid, []boson.Address{cAddress})
	time.Sleep(5 * time.Second)

	// a -> b
	reqBRecords, err := bRecorder.Records(cAddress, protocolName, protocolVersion, streamChunkInfoReqName)
	if err != nil {
		t.Fatal(err)
	}

	if l := len(reqBRecords); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	respRecord := reqBRecords[0]
	reqBMessages, err := protobuf.ReadMessages(
		bytes.NewReader(respRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoReq) },
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(reqBMessages)

	respCRecord, err := recorder.Records(bAddress, protocolName, protocolVersion, streamChunkInfoRespName)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(respCRecord); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	cRecord := respCRecord[0]
	respCMessages, err := protobuf.ReadMessages(
		bytes.NewReader(cRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoResp) },
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(respCMessages)
}

func TestHandlerChunkInfoResp(t *testing.T) {
	bAddress := boson.MustParseHexAddress("02")
	aAddress := boson.MustParseHexAddress("01")

	rootCid, tra := mockUploadFile(t)

	aOverlay := mockChunkInfo(tra, nil, aAddress)
	aRecorder := streamtest.New(streamtest.WithProtocols(aOverlay.Protocol()), streamtest.WithBaseAddr(bAddress))
	aOverlay.newQueue(rootCid.String())
	bOverlay := mockChunkInfo(tra, aRecorder, bAddress)

	ctx := context.Background()

	//resp  b ->a
	tree, _ := bOverlay.getChunkPyramid(ctx, rootCid)
	pram, _ := bOverlay.traversal.CheckTrieData(ctx, rootCid, tree)
	if err := bOverlay.OnChunkTransferred(boson.NewAddress(pram[0][0]), rootCid, bAddress); err != nil {
		t.Fatal(err)
	}
	req := bOverlay.cd.createChunkInfoReq(rootCid, bAddress, aAddress)

	if err := bOverlay.onChunkInfoReq(ctx, nil, aAddress, req); err != nil {
		t.Fatal(err)
	}

	var vb bitVector
	if err := aOverlay.storer.Get(generateKey(discoverKeyPrefix, rootCid, bAddress), &vb); err != nil {
		t.Fatal(err)
	}
	vf, err := bitvector.NewFromBytes(vb.B, vb.Len)
	if err != nil {
		t.Fatal(err)
	}
	if vf.String() != "0000000000100000" {
		t.Fatalf("got %v records, want %v", vf.String(), "0000000001000000")
	}

	respRecords, err := aRecorder.Records(aAddress, "chunkinfo", "1.0.0", "chunkinforesp")
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
	v := aOverlay.GetChunkInfo(rootCid, boson.NewAddress(pram[0][0]))
	if v == nil || len(v) == 0 {
		t.Fatalf("bit vector error")
	}
}

func TestHandlerChunkInfoRespRelay(t *testing.T) {
	// resp c->b->a
	cAddress := boson.MustParseHexAddress("03")
	bAddress := boson.MustParseHexAddress("02")
	aAddress := boson.MustParseHexAddress("01")

	rootCid, tra := mockUploadFile(t)

	aOverlay := mockChunkInfo(tra, nil, aAddress)
	aRecorder := streamtest.New(streamtest.WithProtocols(aOverlay.Protocol()), streamtest.WithBaseAddr(bAddress))
	aOverlay.newQueue(rootCid.String())
	bOverlay := mockChunkInfo(tra, aRecorder, bAddress)
	bOverlay.newQueue(rootCid.String())
	bRecorder := streamtest.New(streamtest.WithProtocols(bOverlay.Protocol()), streamtest.WithBaseAddr(aAddress))
	cOverlay := mockChunkInfo(tra, bRecorder, cAddress)

	resp := cOverlay.ct.createChunkInfoResp(rootCid, nil, cAddress.Bytes(), aAddress.Bytes())
	cOverlay.sendData(context.Background(), bAddress, streamChunkInfoRespName, resp)

	// c -> b
	respCRecords, err := bRecorder.Records(bAddress, protocolName, protocolVersion, streamChunkInfoRespName)
	if err != nil {
		t.Fatal(err)
	}

	if l := len(respCRecords); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	respRecord := respCRecords[0]
	respBMessages, err := protobuf.ReadMessages(
		bytes.NewReader(respRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoResp) },
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(respBMessages)

	// b -> a
	respBRecords, err := aRecorder.Records(aAddress, protocolName, protocolVersion, streamChunkInfoRespName)
	if err != nil {
		t.Fatal(err)
	}

	if l := len(respBRecords); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	respBRecord := respBRecords[0]
	respAMessages, err := protobuf.ReadMessages(
		bytes.NewReader(respBRecord.In()),
		func() protobuf.Message { return new(pb.ChunkInfoResp) },
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(respAMessages)
}

func TestHandlerPyramid(t *testing.T) {
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")
	ctx := context.Background()
	rootCid, s := mockUploadFile(t)
	server := mockChunkInfo(s, nil, serverAddress)
	tree, _ := server.getChunkPyramid(ctx, rootCid)
	pram, _ := server.traversal.CheckTrieData(ctx, rootCid, tree)
	if err := server.OnChunkTransferred(boson.NewAddress(pram[0][1]), rootCid, serverAddress); err != nil {
		t.Fatal(err)
	}
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(clientAddress),
	)
	client := mockChunkInfo(s, recorder, clientAddress)
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

	recordsChunk, err := recorder.Records(serverAddress, "chunkinfo", "1.0.0", "chunkpyramidchunk")
	if err != nil {
		t.Fatal(err)
	}
	recordChunk := recordsChunk[0]
	chunkMessage, err := protobuf.ReadMessages(
		bytes.NewReader(recordChunk.Out()),
		func() protobuf.Message { return new(pb.ChunkPyramidChunkResp) },
	)

	if err != nil {
		t.Fatal(err)
	}
	t.Log(chunkMessage)

	cids := client.cp.getChunkCid(rootCid)
	t.Log(cids)
	if cids == nil || len(cids) == 0 {
		t.Fatalf("chunk pyramid is nil")
	}
}

func TestQueueProcess(t *testing.T) {
	clientAddress := boson.MustParseHexAddress("01")

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
	aAddress := mockChunkInfo(s, recorder, clientAddress)
	aAddress.cpd.updatePendingFinder(rootCid)
	aAddress.newQueue(rc)
	aAddress.getQueue(rc).push(Pulling, addr.Bytes())
	// max pulling
	for i := 0; i < PullMax; i++ {
		overlay := test.RandomAddress()
		if i == 0 {
			aAddress.updateQueue(context.Background(), nil, rootCid, addr, [][]byte{overlay.Bytes()})
		} else {
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

func mockChunkInfo(traversal traversal.Service, r *streamtest.Recorder, overlay boson.Address) *ChunkInfo {
	logger := logging.New(ioutil.Discard, 0)
	ret := smock.NewStateStore()
	rmock := rmock.NewMockRouteTable()
	server := New(overlay, r, logger, traversal, ret, &rmock, "127.0.0.1:8000")
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
