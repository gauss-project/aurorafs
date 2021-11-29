package traffic

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/crypto/eip712"
	signermock "github.com/gauss-project/aurorafs/pkg/crypto/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain/transaction"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"math/big"
	"testing"
)

const (
	protocolName    = "pseudosettle"
	protocolVersion = "1.0.0"
	streamName      = "traffic" // stream for cheques
	initStreamName  = "init"    // stream for handshake
)

type libp2pServiceOpts struct {
	Logger      logging.Logger
	Addressbook addressbook.Interface
	PrivateKey  *ecdsa.PrivateKey
	MockPeerKey *ecdsa.PrivateKey
	libp2pOpts  libp2p.Options
	lightNodes  *lightnode.Container
	bootNodes   *bootnode.Container
}

func TestHandSake(t *testing.T) {
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0123")
	beneficiary := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("03")
	p2pStream := streamtest.New(
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
			}, protocolName, protocolVersion, streamName)),
	)

	trafficService := newTraaffic(t, p2pStream, chainAddress, beneficiary)

	c := chequePkg.Cheque{
		Recipient:        Recipient,
		Beneficiary:      beneficiary,
		CumulativePayout: big.NewInt(10),
	}
	sin, err := trafficService.chequeSigner.Sign(&c)
	if err != nil {
		t.Fatal(err)
	}
	signedCheque := &chequePkg.SignedCheque{
		Cheque:    c,
		Signature: sin,
	}

	err = trafficService.Handshake(peerAddress, beneficiary, signedCheque)
	if err != nil {
		t.Fatal(err)
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

func newTraaffic(t *testing.T, streamer p2p.Streamer, chainAddress common.Address, beneficiary common.Address) *Service {
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	backend, err := ethclient.Dial("https://data-seed-prebsc-1-s1.binance.org:8545/")
	chainID := int64(1)
	trafficChainService, err := chainTraffic.NewServer(logger, backend, "123456789")
	signature := common.Hex2Bytes("abc")
	signer := signermock.New(
		signermock.WithSignTypedDataFunc(func(data *eip712.TypedData) ([]byte, error) {

			if data.Message["beneficiary"].(string) != beneficiary.Hex() {
				t.Fatal("signing cheque with wrong beneficiary")
			}

			return signature, nil
		}),
	)

	transactionService, err := transaction.NewService(logger, backend, signer, store, new(big.Int).SetInt64(chainID))
	if err != nil {
		t.Fatal(err)
	}

	chequeStore := chequePkg.NewChequeStore(store, beneficiary, chequePkg.RecoverCheque, chainID)
	cashOut := chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := NewAddressBook(store)
	protocol := trafficprotocol.New(streamer, logger)
	chequeSigner := chequePkg.NewChequeSigner(signer, chainID)
	p2pServer, _ := newP2pService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{NodeMode: aurora.NewModel()}})

	trafficService := New(
		logger,
		chainAddress,
		store,
		trafficChainService,
		chequeStore,
		cashOut,
		p2pServer,
		addressBook,
		chequeSigner,
		protocol,
		chainID,
	)
	return trafficService
}

// newService constructs a new libp2p service.
func newP2pService(t *testing.T, networkID uint64, o libp2pServiceOpts) (s *libp2p.Service, overlay boson.Address) {

	t.Helper()

	auroraKey, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}

	overlay, err = crypto.NewOverlayAddress(auroraKey.PublicKey, networkID)
	if err != nil {
		t.Fatal(err)
	}

	addr := ":0"

	if o.Logger == nil {
		o.Logger = logging.New(io.Discard, 0)
	}

	statestore := mock.NewStateStore()
	if o.Addressbook == nil {
		o.Addressbook = addressbook.New(statestore)
	}

	if o.PrivateKey == nil {
		libp2pKey, err := crypto.GenerateSecp256k1Key()
		if err != nil {
			t.Fatal(err)
		}

		o.PrivateKey = libp2pKey
	}

	ctx, cancel := context.WithCancel(context.Background())

	if o.lightNodes == nil {
		o.lightNodes = lightnode.NewContainer(overlay)
	}
	if o.bootNodes == nil {
		o.bootNodes = bootnode.NewContainer(overlay)
	}
	opts := o.libp2pOpts

	s, err = libp2p.New(ctx, crypto.NewDefaultSigner(auroraKey), networkID, overlay, addr, o.Addressbook, statestore, o.lightNodes, o.bootNodes, o.Logger, nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Ready()

	t.Cleanup(func() {
		cancel()
		s.Close()
	})
	return s, overlay

}
