package chunkinfo

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/boson/test"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/pb"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	rmock "github.com/gauss-project/aurorafs/pkg/routetab/mock"
	omock "github.com/gauss-project/aurorafs/pkg/settlement/chain/oracle/mock"
	smock "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"golang.org/x/sync/errgroup"
)

const fileContentType = "text/plain; charset=utf-8"

var (
	simpleData = []byte("hello test world") // fixed, 16 bytes
)

func TestInit(t *testing.T) {

	serverAddress := boson.MustParseHexAddress("01")
	rootCid, _ := boson.ParseHexAddress("6aa47f0d31e20784005cb2148b6fed85e538f829698ef552bb590be1bfa7e643")

	_, s := mockUploadFile(t)
	recorder := streamtest.New(
		streamtest.WithBaseAddr(serverAddress),
	)

	server := mockChunkInfo(s, recorder, serverAddress)

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
	reqRecords, err := recorder.Records(serverAddress, protocolName, protocolVersion, streamChunkInfoReqName)

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

	// resp  b ->a
	tree, _ := bOverlay.getChunkPyramid(ctx, rootCid)
	pram, _, _ := bOverlay.traversal.GetChunkHashes(ctx, rootCid, tree)
	if err := bOverlay.OnChunkTransferred(boson.NewAddress(pram[0][0]), rootCid, bAddress, boson.ZeroAddress); err != nil {
		t.Fatal(err)
	}
	req := bOverlay.cd.createChunkInfoReq(rootCid, bAddress, aAddress)

	if err := bOverlay.onChunkInfoReq(ctx, nil, aAddress, req); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	var vb BitVector
	if err := aOverlay.stateStorer.Get(generateKey(discoverKeyPrefix, rootCid, bAddress), &vb); err != nil {
		t.Fatal(err)
	}
	vf, err := bitvector.NewFromBytes(vb.B, vb.Len)
	if err != nil {
		t.Fatal(err)
	}
	if vf.String() != "10000000" {
		t.Fatalf("got %v records, want %v", vf.String(), "100000000")
	}

	respRecords, err := aRecorder.Records(aAddress, protocolName, protocolVersion, streamChunkInfoRespName)
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
	if len(v) == 0 {
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
	err := cOverlay.sendData(context.Background(), bAddress, streamChunkInfoRespName, resp)
	if err != nil {
		t.Fatal(err)
	}

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
	targetAddress := boson.MustParseHexAddress("03")
	serverAddress := boson.MustParseHexAddress("02")
	clientAddress := boson.MustParseHexAddress("01")
	ctx := context.Background()
	rootCid, s := mockUploadFile(t)

	target := mockChunkInfo(s, nil, targetAddress)
	targetRecorder := streamtest.New(
		streamtest.WithProtocols(target.Protocol()),
		streamtest.WithBaseAddr(targetAddress),
	)
	server := mockChunkInfo(s, targetRecorder, serverAddress)
	tree, _ := server.getChunkPyramid(ctx, rootCid)
	pram, _, _ := server.traversal.GetChunkHashes(ctx, rootCid, tree)
	if err := target.OnChunkTransferred(boson.NewAddress(pram[0][1]), rootCid, targetAddress, boson.ZeroAddress); err != nil {
		t.Fatal(err)
	}
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(serverAddress),
	)
	client := mockChunkInfo(s, recorder, clientAddress)
	_ = client.doFindChunkPyramid(context.Background(), nil, rootCid, targetAddress)

	records, err := recorder.Records(targetAddress, protocolName, protocolVersion, streamPyramidName)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(records); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}
	record := records[0]
	messages, err := protobuf.ReadMessages(
		bytes.NewReader(record.In()),
		func() protobuf.Message { return new(pb.ChunkPyramidReq) },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(messages)
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
		m := make(map[string][]byte)
		m[overlay.String()] = nil
		if i == 0 {
			aAddress.updateQueue(context.Background(), nil, rootCid, addr, m)
		} else {
			for i := 0; i < 6; i++ {
				m[test.RandomAddress().String()] = nil
			}
			aAddress.updateQueue(context.Background(), nil, rootCid, boson.NewAddress(*aAddress.getQueue(rc).pop(Pulling)), m)
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

func mockUploadFile(t *testing.T) (boson.Address, traversal.Traverser) {
	chunkCount := 10 + 1

	ctx := context.Background()
	largeBytesData := generateSample(boson.ChunkSize * chunkCount)

	mockStoreA := mock.NewStorer()
	traversalService := traversal.New(mockStoreA)

	reference, _ := uploadFile(t, ctx, mockStoreA, largeBytesData, "", false)
	return reference, traversalService
}

func mockChunkInfo(traversal traversal.Traverser, r *streamtest.Recorder, overlay boson.Address) *ChunkInfo {
	logger := logging.New(io.Discard, 0)
	ret := smock.NewStateStore()
	s := mock.NewStorer()
	route := rmock.NewMockRouteTable()
	oracle := omock.NewServer()
	server := New(overlay, r, logger, traversal, ret, s, &route, oracle, nil, subscribe.NewSubPub())
	err := server.InitChunkInfo()
	if err != nil {
		return nil
	}
	return server
}

func generateSample(size int) (b []byte) {
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
	fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}

	ls := loadsave.New(store, pipelineFactory(store, storage.ModePutRequest, false))
	fManifest, err := manifest.NewDefaultManifest(ls, false)
	if err != nil {
		t.Fatal(err)
	}

	if filename == "" {
		filename = fr.String()
	}

	rootMtdt := map[string]string{
		manifest.WebsiteIndexDocumentSuffixKey: filename,
		manifest.EntryMetadataDirnameKey:       filename,
	}
	err = fManifest.Add(ctx, "/", manifest.NewEntry(boson.ZeroAddress, rootMtdt))
	if err != nil {
		t.Fatal(err)
	}

	fileMtdt := map[string]string{
		manifest.EntryMetadataFilenameKey:    filename,
		manifest.EntryMetadataContentTypeKey: fileContentType,
	}
	err = fManifest.Add(ctx, filename, manifest.NewEntry(fr, fileMtdt))
	if err != nil {
		t.Fatal(err)
	}

	address, err := fManifest.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return address, filename
}

func pipelineFactory(s storage.Putter, mode storage.ModePut, encrypt bool) func() pipeline.Interface {
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(context.Background(), s, mode, encrypt)
	}
}
