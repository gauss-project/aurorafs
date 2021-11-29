package traffic

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/storage"
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

type cashOutMock struct {
	cashCheque func(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error)
}

func (m *cashOutMock) CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error) {
	return m.cashCheque(ctx, chequebook, recipient)
}

type chequeSignerMock struct {
	sign func(cheque *chequePkg.Cheque) ([]byte, error)
}

func (m *chequeSignerMock) Sign(cheque *chequePkg.Cheque) ([]byte, error) {
	return m.sign(cheque)
}

type addressBookMock struct {
	beneficiary     func(peer boson.Address) (beneficiary common.Address, known bool, err error)
	beneficiaryPeer func(beneficiary common.Address) (peer boson.Address, known bool, err error)
	putBeneficiary  func(peer boson.Address, beneficiary common.Address) error
	initAddressBook func() error
}

func (m *addressBookMock) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	return m.beneficiary(peer)
}

func (m *addressBookMock) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	return m.beneficiaryPeer(beneficiary)
}

func (m *addressBookMock) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	return m.putBeneficiary(peer, beneficiary)
}

func (m *addressBookMock) InitAddressBook() error {
	return nil
}

func TestHandSake(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	//chainAddress := common.HexToAddress("0x123")
	chueBeneficiary := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("03")
	sig := common.Hex2Bytes(chueBeneficiary.String())
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

	logger := logging.New(ioutil.Discard, 0)
	transactionService, err := chainTraffic.NewServer(logger, nil, "") //transaction.NewService(logger, nil, Signer, nil, new(big.Int).SetInt64(10))
	if err != nil {
		t.Fatal(err)
	}
	chequestore := chequePkg.NewChequeStore(
		store,
		chueBeneficiary,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return chueBeneficiary, nil
		},
		chainID)

	cashOut := cashOutMock{}           //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	traddressBook := addressBookMock{} //NewAddressBook(store)
	traddressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
		return chueBeneficiary, true, nil
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	c := chequePkg.Cheque{
		Recipient:        Recipient,
		Beneficiary:      chueBeneficiary,
		CumulativePayout: big.NewInt(10),
	}
	sin, err := chequeSigner.Sign(&c)
	if err != nil {
		t.Fatal(err)
	}
	signedCheque := &chequePkg.SignedCheque{
		Cheque:    c,
		Signature: sin,
	}

	trafficService := newTraaffic(t, store, logger, p2pStream, chueBeneficiary, chueBeneficiary, transactionService, chequestore, &cashOut, &traddressBook, &chequeSigner, chainID)

	err = trafficService.Handshake(peerAddress, chueBeneficiary, signedCheque)
	if err != nil {
		t.Fatal(err)
	}

}

func TestPayUnknownBeneficiary(t *testing.T) {

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

func newTraaffic(t *testing.T, store storage.StateStorer, logger logging.Logger, streamer p2p.Streamer, chainAddress common.Address, beneficiary common.Address,
	trafficChainService chain.Traffic, chequeStore chequePkg.ChequeStore, cashout chequePkg.CashoutService,
	addressBook Addressbook, chequeSigner chequePkg.ChequeSigner, chainID int64) *Service {

	protocol := trafficprotocol.New(streamer, logger)
	p2pServer, _ := newP2pService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{NodeMode: aurora.NewModel()}})

	trafficService := New(
		logger,
		chainAddress,
		store,
		trafficChainService,
		chequeStore,
		cashout,
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
