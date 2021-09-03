package retrieval_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	// "fmt"
	"io/ioutil"
	// "sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/logging"

	// "github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	pb "github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	storemock "github.com/gauss-project/aurorafs/pkg/storage/mock"
	testingc "github.com/gauss-project/aurorafs/pkg/storage/testing"
	// "github.com//pkg/swarm"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/topology"
)

var (
	testTimeout  = 5 * time.Second
	defaultPrice = uint64(10)
)

func TestDelivery(t *testing.T) {
	var (
		chunk                = testingc.FixtureChunk("0033")
		rootAddr			 = boson.MustParseHexAddress("0003")
		logger               = logging.New(ioutil.Discard, 0)
		mockStorer           = storemock.NewStorer()
		clientAddr           = boson.MustParseHexAddress("9ee7add8")
		serverAddr           = boson.MustParseHexAddress("9ee7add7")
	)

	// put testdata in the mock store of the server
	_, err := mockStorer.Put(context.Background(), storage.ModePutUpload, chunk)
	if err != nil {
		t.Fatal(err)
	}

	// create the server that will handle the request and will serve the response
	server := retrieval.New(boson.MustParseHexAddress("0034"), nil, nil, mockStorer, logger, nil)

	// server := retrieval.New(boson.MustParseHexAddress("0034"), mockStorer, nil, nil, logger, serverMockAccounting, pricerMock, nil, false, noopStampValidator)
	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(clientAddr),
	)

	// client mock storer does not store any data at this point
	// but should be checked at at the end of the test for the
	// presence of the chunk address key and value to ensure delivery
	// was successful
	clientMockStorer := storemock.NewStorer()

	ps := mockPeerSuggester{eachPeerRevFunc: func(f topology.EachPeerFunc) error {
		_, _, _ = f(serverAddr, 0)
		return nil
	}}

	mockChunkinfo := chunkinfo.New(nil, logger,nil, )
	client := retrieval.New(clientAddr, recorder, ps, clientMockStorer, logger, chunkinfo)
	// client := retrieval.New(clientAddr, clientMockStorer, recorder, ps, logger, clientMockAccounting, pricerMock, nil, false, noopStampValidator)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	// v, err := client.RetrieveChunk(ctx, chunk.Address(), true)
	v, err := client.RetrieveChunk(ctx, rootAddr, chunk.Address())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v.Data(), chunk.Data()) {
		t.Fatalf("request and response data not equal. got %s want %s", v, chunk.Data())
	}
	records, err := recorder.Records(serverAddr, "retrieval", "1.0.0", "retrieval")
	if err != nil {
		t.Fatal(err)
	}
	if l := len(records); l != 1 {
		t.Fatalf("got %v records, want %v", l, 1)
	}

	record := records[0]

	messages, err := protobuf.ReadMessages(
		bytes.NewReader(record.In()),
		func() protobuf.Message { return new(pb.Request) },
	)
	if err != nil {
		t.Fatal(err)
	}
	var reqs []string
	for _, m := range messages {
		reqs = append(reqs, hex.EncodeToString(m.(*pb.Request).Addr))
	}

	if len(reqs) != 1 {
		t.Fatalf("got too many requests. want 1 got %d", len(reqs))
	}

	messages, err = protobuf.ReadMessages(
		bytes.NewReader(record.Out()),
		func() protobuf.Message { return new(pb.Delivery) },
	)
	if err != nil {
		t.Fatal(err)
	}
	var gotDeliveries []string
	for _, m := range messages {
		gotDeliveries = append(gotDeliveries, string(m.(*pb.Delivery).Data))
	}

	if len(gotDeliveries) != 1 {
		t.Fatalf("got too many deliveries. want 1 got %d", len(gotDeliveries))
	}
}

type mockPeerSuggester struct {
	eachPeerRevFunc func(f topology.EachPeerFunc) error
}

func (s mockPeerSuggester) EachPeer(topology.EachPeerFunc) error {
	return errors.New("not implemented")
}

func (s mockPeerSuggester) EachPeerRev(f topology.EachPeerFunc) error {
	return s.eachPeerRevFunc(f)
}

var noopStampValidator = func(chunk boson.Chunk, stampBytes []byte) (boson.Chunk, error) {
	return chunk, nil
}

func mockChunkInfo(traversal traversal.Service, r *streamtest.Recorder) *ChunkInfo {
	logger := logging.New(ioutil.Discard, 0)
	server := chunkinfo.New(r, logger, traversal, "127.0.0.1:8000")
	return server
}