package retrieval_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo/mock"
	rmock "github.com/gauss-project/aurorafs/pkg/routetab/mock"
	"sync"

	"io"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/logging"
	// "github.com/gauss-project/aurorafs/pkg/cac"
	// "github.com/gauss-project/aurorafs/pkg/soc"

	"github.com/gauss-project/aurorafs/pkg/bmtpool"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/retrieval"
	pb "github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
	storemock "github.com/gauss-project/aurorafs/pkg/storage/mock"
	// "io"
	// "os"
	// "github.com/sirupsen/logrus"
)

var (
	testTimeout = 5 * time.Second
	// defaultPrice = uint64(10)
)

// TestDelivery tests that a naive request -> delivery flow works.
func TestDelivery(t *testing.T) {
	var (
		// chunk                = testingc.FixtureChunk("0033")
		rootAddr   = boson.MustParseHexAddress("3300")
		logger     = logging.New(io.Discard, 0)
		mockStorer = storemock.NewStorer()
		clientAddr = boson.MustParseHexAddress("9ee7add8")
		serverAddr = boson.MustParseHexAddress("9ee7add7")
	)

	bmtHashOfFoo := "8a74889a73c23fe2be037886c6b709e3175b95b8deea9c95eeda0dbc60740bd8"
	address := boson.MustParseHexAddress(bmtHashOfFoo)
	data := []uint8{3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111}

	chunk := boson.NewChunk(address, data)

	// put testdata in the mock store of the server
	_, err := mockStorer.Put(context.Background(), storage.ModePutUpload, chunk)
	if err != nil {
		t.Fatal(err)
	}

	// create the server that will handle the request and will serve the response
	mockRouteTable := rmock.NewMockRouteTable()
	server := retrieval.New(serverAddr, nil, &mockRouteTable, mockStorer, true, logger, nil)
	serverMockChunkInfo := mock.New(mockRouteTable)
	server.Config(serverMockChunkInfo)

	recorder := streamtest.New(
		streamtest.WithProtocols(server.Protocol()),
		streamtest.WithBaseAddr(serverAddr),
		// streamtest.WithBaseAddr(clientAddr),
	)

	// client mock storer does not store any data at this point
	// but should be checked at at the end of the test for the
	// presence of the chunk address key and value to ensure delivery
	// was successful
	clientMockStorer := storemock.NewStorer()

	// ps := mockPeerSuggester{eachPeerRevFunc: func(f topology.EachPeerFunc) error {
	// 	_, _, _ = f(serverAddr, 0)
	// 	return nil
	// }}

	mockChunkInfo := mock.New(mockRouteTable)
	err = mockChunkInfo.OnChunkTransferred(chunk.Address(), rootAddr, serverAddr, boson.ZeroAddress)
	if err != nil {
		t.Fatal(err)
	}

	client := retrieval.New(clientAddr, recorder, &mockRouteTable, clientMockStorer, true, logger, nil)
	client.Config(mockChunkInfo)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	v, err := client.RetrieveChunk(ctx, rootAddr, chunk.Address())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v.Data(), chunk.Data()) {
		t.Fatalf("request and response data not equal. got %s want %s", v, chunk.Data())
	}
	// vstamp, err := v.Stamp().MarshalBinary()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if !bytes.Equal(vstamp, stamp) {
	// 	t.Fatal("stamp mismatch")
	// }
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

	// clientBalance, _ := clientMockAccounting.Balance(serverAddr)
	// if clientBalance.Int64() != -int64(defaultPrice) {
	// 	t.Fatalf("unexpected balance on client. want %d got %d", -defaultPrice, clientBalance)
	// }

	// serverBalance, _ := serverMockAccounting.Balance(clientAddr)
	// if serverBalance.Int64() != int64(defaultPrice) {
	// 	t.Fatalf("unexpected balance on server. want %d got %d", defaultPrice, serverBalance)
	// }
}

func TestRetrievalChunk(t *testing.T) {
	// logger := logging.New(io.MultiWriter(os.Stdout), 6)
	// logFormater := logrus.TextFormatter{
	// 	DisableTimestamp: true,
	// 	ForceColors:      true,
	// }
	// logger.NewEntry().Logger.SetFormatter(&logFormater)
	logger := logging.New(io.Discard, 0)
	var (
		mockRouteTable = rmock.NewMockRouteTable()
	)

	t.Run("downstream", func(t *testing.T) {
		serverAddress := boson.MustParseHexAddress("0003")
		clientAddress := boson.MustParseHexAddress("0001")
		// chunk := testingc.FixtureChunk("02c2")

		chunkBytes := []uint8{3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111}
		h := hasher(chunkBytes)
		addressBytes, _ := h(chunkBytes[:boson.SpanSize])
		chunkAddr := boson.NewAddress(addressBytes)
		chunk := boson.NewChunk(chunkAddr, chunkBytes)

		rootAddr := boson.MustParseHexAddress("0101")

		serverStorer := storemock.NewStorer()
		_, err := serverStorer.Put(context.Background(), storage.ModePutUpload, chunk)
		if err != nil {
			t.Fatal(err)
		}

		serverMockChunkInfo := mock.New(rmock.MockRouteTable{})
		server := retrieval.New(serverAddress, nil, &mockRouteTable, serverStorer, true, logger, nil)
		server.Config(serverMockChunkInfo)

		recorder := streamtest.New(
			streamtest.WithProtocols(server.Protocol()),
			streamtest.WithBaseAddr(clientAddress),
		)

		clientStorer := storemock.NewStorer()
		clientMockChunkInfo := mock.New(rmock.MockRouteTable{})
		err = clientMockChunkInfo.OnChunkTransferred(chunk.Address(), rootAddr, serverAddress, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}
		client := retrieval.New(clientAddress, recorder, &mockRouteTable, clientStorer, true, logger, nil)
		client.Config(clientMockChunkInfo)

		got, err := client.RetrieveChunk(context.Background(), rootAddr, chunk.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Data(), chunk.Data()) {
			t.Fatalf("got data %x, want %x", got.Data(), chunk.Data())
		}
	})

	t.Run("forward", func(t *testing.T) {
		// client =====> forwarder =====> server
		address := boson.MustParseHexAddress("8a74889a73c23fe2be037886c6b709e3175b95b8deea9c95eeda0dbc60740bd8")
		data := []uint8{3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111}

		chunk := boson.NewChunk(address, data)
		rootAddr := boson.MustParseHexAddress("0101")

		serverAddress := boson.MustParseHexAddress("0001")
		forwarderAddress := boson.MustParseHexAddress("0002")
		clientAddress := boson.MustParseHexAddress("0003")

		// config server
		serverStorer := storemock.NewStorer()
		_, err := serverStorer.Put(context.Background(), storage.ModePutUpload, chunk)
		if err != nil {
			t.Fatal(err)
		}
		server := retrieval.New(serverAddress, nil, &mockRouteTable, serverStorer, true, logger, nil)

		// config forwarder
		f2sRecorder := streamtest.New(
			streamtest.WithProtocols(server.Protocol()),
			streamtest.WithBaseAddr(forwarderAddress),
		)
		forwarderStorer := storemock.NewStorer()
		forwarder := retrieval.New(forwarderAddress, f2sRecorder, &mockRouteTable, forwarderStorer, true, logger, nil)

		// config client
		c2fRecorder := streamtest.New(
			streamtest.WithProtocols(forwarder.Protocol()),
			streamtest.WithBaseAddr(clientAddress),
		)
		clientStorer := storemock.NewStorer()
		client := retrieval.New(clientAddress, c2fRecorder, &mockRouteTable, clientStorer, true, logger, nil)

		clientChunkInfo := mock.New(rmock.MockRouteTable{})
		client.Config(clientChunkInfo)
		forwarder.Config(clientChunkInfo)
		err = clientChunkInfo.OnChunkTransferred(chunk.Address(), rootAddr, serverAddress, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}

		if got, _ := forwarderStorer.Has(context.Background(), storage.ModeHasChunk, chunk.Address()); got {
			t.Fatalf("forwarder node already has chunk")
		}

		got, err := client.RetrieveChunk(context.Background(), rootAddr, chunk.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Data(), chunk.Data()) {
			t.Fatalf("got data %x, want %x", got.Data(), chunk.Data())
		}

		if got, _ := forwarderStorer.Has(context.Background(), storage.ModeHasChunk, chunk.Address()); !got {
			t.Fatalf("forwarder did not cache chunk")
		}
	})

}

func TestNeighborRetrieval(t *testing.T) {
	// logger := logging.New(io.MultiWriter(os.Stdout), 6)
	// logFormater := logrus.TextFormatter{
	// 	DisableTimestamp: true,
	// 	ForceColors:      true,
	// }
	// logger.NewEntry().Logger.SetFormatter(&logFormater)

	logger := logging.New(io.Discard, 0)

	address := boson.MustParseHexAddress("8a74889a73c23fe2be037886c6b709e3175b95b8deea9c95eeda0dbc60740bd8")
	data := []uint8{3, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111}

	chunk := boson.NewChunk(address, data)
	chunkAddr := chunk.Address()
	rootAddr := boson.MustParseHexAddress("1001")

	t.Run("relay node contains chunk", func(t *testing.T) {
		// client retrieval => relay => server
		serverAddress := boson.MustParseHexAddress("0001")
		neighborServerAddress := boson.MustParseHexAddress("0002")
		clientAddress := boson.MustParseHexAddress("0003")

		// init server
		serverStorer := storemock.NewStorer()
		if _, err := serverStorer.Put(context.Background(), storage.ModePutUpload, chunk); err != nil {
			t.Fatal(err)
		}
		server := retrieval.New(serverAddress, nil, &rmock.MockRouteTable{}, serverStorer, true, logger, nil)
		neighbor2serverRecorder := streamtest.New(
			streamtest.WithProtocols(
				server.Protocol(),
			),
			streamtest.WithBaseAddr(
				neighborServerAddress,
			),
		)

		// init neighbor
		neighborStorer := storemock.NewStorer()
		clientStorer := storemock.NewStorer()
		// if _, err := relayStorer.Put(context.Background(), storage.ModePutUpload, chunk); err != nil{
		// 	t.Fatal(err)
		// }
		neighborServer := retrieval.New(neighborServerAddress, neighbor2serverRecorder, &rmock.MockRouteTable{}, neighborStorer, true, logger, nil)
		client2neighborRecorder := streamtest.New(
			streamtest.WithProtocols(
				neighborServer.Protocol(),
			),
			streamtest.WithBaseAddr(
				clientAddress,
			),
		)

		// init client
		clientRouteTable := rmock.MockRouteTable{
			RejectAddrList: []boson.Address{serverAddress},
			NeighborMap: map[string][]boson.Address{
				serverAddress.String(): {neighborServerAddress},
			},
		}
		client := retrieval.New(clientAddress, client2neighborRecorder, &clientRouteTable, clientStorer, true, logger, nil)

		clientChunkInfo := mock.New(clientRouteTable)
		err := clientChunkInfo.OnChunkTransferred(chunkAddr, rootAddr, serverAddress, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}
		neighborServer.Config(clientChunkInfo)
		client.Config(clientChunkInfo)

		got, err := client.RetrieveChunk(context.Background(), rootAddr, chunk.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Data(), chunk.Data()) {
			t.Fatalf("got data %x, want %x", got.Data(), chunk.Data())
		}

		if got, _ := neighborStorer.Has(context.Background(), storage.ModeHasChunk, chunk.Address()); !got {
			t.Fatalf("neighbor did not cache chunk")
		}
	})
}

func TestRetrievePreemptiveRetry(t *testing.T) {
	// logger := logging.New(io.MultiWriter(os.Stdout), 6)
	// logFormater := logrus.TextFormatter{
	// 	DisableTimestamp: true,
	// 	ForceColors: true,
	// }
	// logger.NewEntry().Logger.SetFormatter(&logFormater)
	logger := logging.New(io.Discard, 0)

	var fixtureChunks = map[string]boson.Chunk{
		"c8ea": boson.NewChunk(
			boson.MustParseHexAddress("c8eaf98c8a8d62c6591d00f9d7b306805c5c0953b706d9ce8c66b90ba230687b"),
			[]byte{72, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 149, 179, 31, 244, 146, 247, 129, 123, 132, 248, 215, 77, 44, 47, 91, 248, 229, 215, 89, 156, 210, 243, 3, 110, 204, 74, 101, 119, 53, 53, 145, 188, 193, 153, 130, 197, 83, 152, 36, 140, 150, 209, 191, 214, 193, 4, 144, 121, 32, 45, 205, 220, 59, 227, 28, 43, 161, 51, 108, 14, 106, 180, 135, 2},
		),
		"2989": boson.NewChunk(
			boson.MustParseHexAddress("2989f0cb15303f352231c35585a4f8012e67bdb20ab337d98c545e4c9dda395f"),
			[]byte{72, 0, 0, 0, 0, 0, 0, 0, 170, 117, 0, 0, 0, 0, 0, 0, 21, 157, 63, 86, 45, 17, 166, 184, 47, 126, 58, 172, 242, 77, 153, 249, 97, 5, 107, 244, 23, 153, 220, 255, 254, 47, 209, 24, 63, 58, 126, 142, 41, 79, 201, 182, 178, 227, 235, 223, 63, 11, 220, 155, 40, 181, 56, 204, 91, 44, 51, 185, 95, 155, 245, 235, 187, 250, 103, 49, 139, 184, 46, 199},
		),
		"4b4e": boson.NewChunk(
			boson.MustParseHexAddress("4b4e98bceed166e1089b79ca6bff553c9dba00bd016c8a4c98bb379f6065a688"),
			[]byte{72, 0, 0, 0, 0, 0, 0, 0, 226, 0, 0, 0, 0, 0, 0, 0, 67, 234, 252, 231, 229, 11, 121, 163, 131, 171, 41, 107, 57, 191, 221, 32, 62, 204, 159, 124, 116, 87, 30, 244, 99, 137, 121, 248, 119, 56, 74, 102, 140, 73, 178, 7, 151, 22, 47, 126, 173, 30, 43, 7, 61, 187, 13, 236, 59, 194, 245, 18, 25, 237, 106, 125, 78, 241, 35, 34, 116, 154, 105, 205},
		),
		"f4c4": boson.NewChunk(
			boson.MustParseHexAddress("f4c4521e86ba4e00780a37c4f34b7f7162de732bf7ce31d54c8f753ded04fc39"),
			[]byte{72, 0, 0, 0, 0, 0, 0, 0, 124, 59, 0, 0, 0, 0, 0, 0, 44, 67, 19, 101, 42, 213, 4, 209, 212, 189, 107, 244, 111, 22, 230, 24, 245, 103, 227, 165, 88, 74, 50, 11, 143, 197, 220, 118, 175, 24, 169, 193, 15, 40, 225, 196, 246, 151, 1, 45, 86, 7, 36, 99, 156, 86, 83, 29, 46, 207, 115, 112, 126, 88, 101, 128, 153, 113, 30, 27, 50, 232, 77, 215},
		),
	}

	chunk := fixtureChunks["c8ea"]
	chunkRootAddr := boson.MustParseHexAddress("c8ea")

	someOtherChunk := fixtureChunks["2989"]
	// someOtherChunkRootAddr := boson.MustParseHexAddress("2989")

	clientAddress := boson.MustParseHexAddress("1010")

	serverAddress1 := boson.MustParseHexAddress("0001")
	serverAddress2 := boson.MustParseHexAddress("0002")

	serverStorer1 := storemock.NewStorer()
	serverStorer2 := storemock.NewStorer()
	clientStorer := storemock.NewStorer()
	// we put some other chunk on server 1
	// _, err := serverStorer1.Put(context.Background(), storage.ModePutUpload, chunk)
	_, err := serverStorer1.Put(context.Background(), storage.ModePutUpload, someOtherChunk)
	if err != nil {
		t.Fatal(err)
	}
	// we put chunk we need on server 2
	_, err = serverStorer2.Put(context.Background(), storage.ModePutUpload, chunk)
	if err != nil {
		t.Fatal(err)
	}

	defaultMockRouteTable := rmock.NewMockRouteTable()

	server1RouteTable := rmock.MockRouteTable{
		RejectAddrList: []boson.Address{serverAddress2},
		NeighborMap:    map[string][]boson.Address{},
	}

	server1 := retrieval.New(serverAddress1, nil, &server1RouteTable, serverStorer1, true, logger, nil)
	server1ChunkInfo := mock.New(defaultMockRouteTable)
	server1.Config(server1ChunkInfo)

	server2 := retrieval.New(serverAddress2, nil, &defaultMockRouteTable, serverStorer2, true, logger, nil)
	server2ChunkInfo := mock.New(defaultMockRouteTable)
	server2.Config(server2ChunkInfo)

	t.Run("peer does not have chunk", func(t *testing.T) {
		ranOnce := true
		ranMux := sync.Mutex{}
		recorder := streamtest.New(
			streamtest.WithProtocols(
				server1.Protocol(),
				server2.Protocol(),
			),
			streamtest.WithMiddlewares(
				func(h p2p.HandlerFunc) p2p.HandlerFunc {
					return func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
						ranMux.Lock()
						defer ranMux.Unlock()
						if ranOnce {
							ranOnce = false
							// logger.Trace("Round 1\n")
							// return fmt.Errorf("peer not reachable: %s", peer.Address.String())
							return server1.Handler(ctx, peer, stream)
						}
						// logger.Trace("Round 2\n")
						return server2.Handler(ctx, peer, stream)
					}
				},
			),
		)

		client := retrieval.New(clientAddress, recorder, &defaultMockRouteTable, clientStorer, true, logger, nil)
		clientChunkInfo := mock.New(defaultMockRouteTable)

		err := clientChunkInfo.OnChunkTransferred(chunk.Address(), chunkRootAddr, serverAddress2, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}
		client.Config(clientChunkInfo)

		got, err := client.RetrieveChunk(context.Background(), chunkRootAddr, chunk.Address())
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(got.Data(), chunk.Data()) {
			t.Fatalf("got data %x, want %x", got.Data(), chunk.Data())
		}
	})

	t.Run("peer not reachable", func(t *testing.T) {
		ranOnce := true
		ranMux := sync.Mutex{}
		recorder := streamtest.New(
			streamtest.WithProtocols(
				server1.Protocol(),
				server2.Protocol(),
			),
			streamtest.WithMiddlewares(
				func(h p2p.HandlerFunc) p2p.HandlerFunc {
					return func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
						ranMux.Lock()
						defer ranMux.Unlock()
						// NOTE: return error for peer1
						if ranOnce {
							ranOnce = false
							return fmt.Errorf("peer not reachable: %s", peer.Address.String())
						}

						return server2.Handler(ctx, peer, stream)
					}
				},
			),
			streamtest.WithBaseAddr(clientAddress),
		)

		// client := retrieval.New(clientAddress, nil, recorder, peerSuggesterFn(peers...), logger, accountingmock.NewAccounting(), pricerMock, nil, false, noopStampValidator)
		mockRouteTable := rmock.NewMockRouteTable()
		client := retrieval.New(clientAddress, recorder, &mockRouteTable, clientStorer, true, logger, nil)

		clientChunkInfo := mock.New(mockRouteTable)
		err := clientChunkInfo.OnChunkTransferred(chunk.Address(), chunkRootAddr, serverAddress1, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}
		err = clientChunkInfo.OnChunkTransferred(chunk.Address(), chunkRootAddr, serverAddress2, boson.ZeroAddress)
		if err != nil {
			t.Fatal(err)
		}
		client.Config(clientChunkInfo)

		got, err := client.RetrieveChunk(context.Background(), chunkRootAddr, chunk.Address())
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(got.Data(), chunk.Data()) {
			t.Fatalf("got data %x, want %x", got.Data(), chunk.Data())
		}
	})
}

// func (r *mockRouteTable)appendRejectAddress(addr boson.Address){
// 	r.rejectAddrList = append(r.rejectAddrList, addr)
// }

//type mockPeerSuggester struct {
//	eachPeerRevFunc func(f topology.EachPeerFunc) error
//}
//
//func (s mockPeerSuggester) EachPeer(topology.EachPeerFunc) error {
//	return errors.New("not implemented")
//}
//
//func (s mockPeerSuggester) EachPeerRev(f topology.EachPeerFunc) error {
//	return s.eachPeerRevFunc(f)
//}

// hasher is a helper function to hash a given data based on the given span.
func hasher(data []byte) func([]byte) ([]byte, error) {
	return func(span []byte) ([]byte, error) {
		hasher := bmtpool.Get()
		defer bmtpool.Put(hasher)

		hasher.SetHeader(span)
		if _, err := hasher.Write(data[boson.SpanSize:]); err != nil {
			return nil, err
		}
		return hasher.Sum(nil), nil
	}
}

func TestBasic(t *testing.T) {
	// chunkByteList := [][]byte{
	// 	[]byte{72, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 149, 179, 31, 244, 146, 247, 129, 123, 132, 248, 215, 77, 44, 47, 91, 248, 229, 215, 89, 156, 210, 243, 3, 110, 204, 74, 101, 119, 53, 53, 145, 188, 193, 153, 130, 197, 83, 152, 36, 140, 150, 209, 191, 214, 193, 4, 144, 121, 32, 45, 205, 220, 59, 227, 28, 43, 161, 51, 108, 14, 106, 180, 135, 2},
	// 	[]byte{72, 0, 0, 0, 0, 0, 0, 0, 170, 117, 0, 0, 0, 0, 0, 0, 21, 157, 63, 86, 45, 17, 166, 184, 47, 126, 58, 172, 242, 77, 153, 249, 97, 5, 107, 244, 23, 153, 220, 255, 254, 47, 209, 24, 63, 58, 126, 142, 41, 79, 201, 182, 178, 227, 235, 223, 63, 11, 220, 155, 40, 181, 56, 204, 91, 44, 51, 185, 95, 155, 245, 235, 187, 250, 103, 49, 139, 184, 46, 199},
	// 	[]byte{72, 0, 0, 0, 0, 0, 0, 0, 226, 0, 0, 0, 0, 0, 0, 0, 67, 234, 252, 231, 229, 11, 121, 163, 131, 171, 41, 107, 57, 191, 221, 32, 62, 204, 159, 124, 116, 87, 30, 244, 99, 137, 121, 248, 119, 56, 74, 102, 140, 73, 178, 7, 151, 22, 47, 126, 173, 30, 43, 7, 61, 187, 13, 236, 59, 194, 245, 18, 25, 237, 106, 125, 78, 241, 35, 34, 116, 154, 105, 205},
	// 	[]byte{72, 0, 0, 0, 0, 0, 0, 0, 124, 59, 0, 0, 0, 0, 0, 0, 44, 67, 19, 101, 42, 213, 4, 209, 212, 189, 107, 244, 111, 22, 230, 24, 245, 103, 227, 165, 88, 74, 50, 11, 143, 197, 220, 118, 175, 24, 169, 193, 15, 40, 225, 196, 246, 151, 1, 45, 86, 7, 36, 99, 156, 86, 83, 29, 46, 207, 115, 112, 126, 88, 101, 128, 153, 113, 30, 27, 50, 232, 77, 215},
	// }
	// for _, c := range chunkByteList{
	// 	h := hasher(c[boson.SpanSize:])
	// 	addressBytes, _ := h(c[:boson.SpanSize])
	// 	address := boson.NewAddress(addressBytes)
	// 	fmt.Printf("%v\n", address)
	// }

	// fooBytes := []uint8{3,0,0,0,0,0,0,0,102,111,111}
	// h := hasher(fooBytes[boson.SpanSize:])
	// addressBytes, _ := h(fooBytes[:boson.SpanSize])
	// address := boson.NewAddress(addressBytes)

	// // data := []uint8{3,0,0,0,0,0,0,0,102,111,111}
	// ch := boson.NewChunk(address, fooBytes)

	// if !cac.Valid(ch) {
	// 	// t.Fatalf("data '%s' should have validated to hash '%s'", ch.Data(), ch.Address())
	// 	fmt.Println("cac failed")
	// 	if !soc.Valid(ch) {
	// 		// t.Fatal("valid chunk evaluates to invalid")
	// 		fmt.Println("soc failed")
	// 	}else{
	// 		fmt.Println("soc pass")
	// 	}
	// }else{
	// 	fmt.Println("cac passed")
	// }
}
