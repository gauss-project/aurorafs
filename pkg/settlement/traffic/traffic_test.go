package traffic

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/crypto/eip712"
	signermock "github.com/gauss-project/aurorafs/pkg/crypto/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	mockp2p "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	mockchequestore "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque/mock"
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

func TestAll(t *testing.T) {
	TestHandSake(t)
	TestPayUnknownBeneficiary(t)
	TestPay(t)
	TestPayIssueError(t)
	TestCashOut(t)
	TestSignCheque(t)
}
func TestHandSake(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(Recipient.String())
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
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return Recipient, nil
		},
		chainID)

	cashOut := cashOutMock{}           //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	traddressBook := addressBookMock{} //NewAddressBook(store)
	traddressBook.putBeneficiary = func(peer boson.Address, beneficiary common.Address) error {
		return nil
	}
	traddressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
		return common.Address{}, false, storage.ErrNotFound
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	c := chequePkg.Cheque{
		Recipient:        Recipient,
		Beneficiary:      chainAddress,
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

	trafficService := newTraffic(t, store, logger, p2pStream, Recipient, transactionService, chequestore, &cashOut, &traddressBook, &chequeSigner, chainID)

	err = trafficService.Handshake(peerAddress, Recipient, signedCheque)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("TestHandSake - Success")

}

func TestPayUnknownBeneficiary(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	peer := boson.MustParseHexAddress("abcd")
	chainAddress := common.HexToAddress("05")
	chainID := int64(1)
	addressBook := &addressBookMock{
		beneficiary: func(p boson.Address) (common.Address, bool, error) {
			if !peer.Equal(p) {
				t.Fatal("querying beneficiary for wrong peer")
			}
			return chainAddress, true, nil
		},
	}
	chequeSigner := &chequeSignerMock{}
	cashOutMock := &cashOutMock{}
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
	protocol := trafficprotocol.New(p2pStream, logger)

	var disconnectCalled bool
	trafficService := New(
		logger,
		chainAddress,
		store,
		nil,
		mockchequestore.NewChequeStore(),
		cashOutMock,
		mockp2p.New(
			mockp2p.WithDisconnectFunc(func(overlay boson.Address, reason string) error {
				if !peer.Equal(overlay) {
					t.Fatalf("disconnecting wrong peer. wanted %v, got %v", peer, overlay)
				}
				disconnectCalled = true
				return nil
			}),
		),
		addressBook,
		chequeSigner,
		protocol,
		chainID,
	)
	trafficService.trafficPeers = TrafficPeer{}
	traffic := Traffic{
		trafficPeerBalance:    new(big.Int).SetInt64(100),
		retrieveChainTraffic:  new(big.Int).SetInt64(0),
		transferChainTraffic:  new(big.Int).SetInt64(0),
		retrieveChequeTraffic: new(big.Int).SetInt64(0),
		transferChequeTraffic: new(big.Int).SetInt64(0),
		retrieveTraffic:       new(big.Int).SetInt64(0),
		transferTraffic:       new(big.Int).SetInt64(0),
	}

	trafficService.trafficPeers.trafficPeers.Store(chainAddress.String(), traffic)
	trafficService.trafficPeers.balance = new(big.Int).SetInt64(0)
	err := trafficService.Pay(context.Background(), peer, big.NewInt(50), big.NewInt(50))
	if !errors.Is(err, ErrUnknownBeneficary) && !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("wrong error. wanted %v, got %v", ErrUnknownBeneficary, err)
	}

	if !disconnectCalled {
		t.Fatal("disconnect was not called")
	}

	//store := mock.NewStateStore()
	//chainID := int64(1)
	//peerAddress := boson.MustParseHexAddress("03")
	//chainAddress := common.HexToAddress("05")
	//Recipient := common.HexToAddress("0xab")
	//sig := common.Hex2Bytes(Recipient.String())
	//p2pStream := streamtest.New(
	//	streamtest.WithProtocols(
	//		newTestProtocol(func(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	//			if _, err := bufio.NewReader(stream).ReadString('\n'); err != nil {
	//				return err
	//			}
	//			var g errgroup.Group
	//			g.Go(stream.Close)
	//			g.Go(stream.FullClose)
	//
	//			if err := g.Wait(); err != nil {
	//				return err
	//			}
	//			return stream.FullClose()
	//		}, protocolName, protocolVersion, streamName)),
	//)
	//
	//logger := logging.New(ioutil.Discard, 0)
	//transactionService, err := chainTraffic.NewServer(logger, nil, "") //transaction.NewService(logger, nil, Signer, nil, new(big.Int).SetInt64(10))
	//if err != nil {
	//	t.Fatal(err)
	//}
	//chequestore := chequePkg.NewChequeStore(
	//	store,
	//	chainAddress,
	//	func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
	//		return Recipient, nil
	//	},
	//	chainID)
	//
	//cashOut := cashOutMock{}           //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	//traddressBook := addressBookMock{} //NewAddressBook(store)
	//traddressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	//	return Recipient, true, nil
	//}
	//chequeSigner := chequeSignerMock{}
	//chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
	//	return sig, nil
	//}
	//
	//c := chequePkg.Cheque{
	//	Recipient:        Recipient,
	//	Beneficiary:      Recipient,
	//	CumulativePayout: big.NewInt(10),
	//}
	//sin, err := chequeSigner.Sign(&c)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//signedCheque := &chequePkg.SignedCheque{
	//	Cheque:    c,
	//	Signature: sin,
	//}
	//
	//trafficService := newTraffic(t, store, logger, p2pStream, chainAddress, transactionService, chequestore, &cashOut, &traddressBook, &chequeSigner, chainID)
	//trafficService.trafficPeers = TrafficPeer{}
	//traffic := Traffic{
	//	trafficPeerBalance: new(big.Int).SetInt64(100),
	//	retrieveChainTraffic: new(big.Int).SetInt64(0),
	//	transferChainTraffic: new(big.Int).SetInt64(0),
	//	retrieveChequeTraffic: new(big.Int).SetInt64(0),
	//	transferChequeTraffic: new(big.Int).SetInt64(0),
	//	retrieveTraffic: new(big.Int).SetInt64(0),
	//	transferTraffic: new(big.Int).SetInt64(0),
	//}
	//
	//trafficService.trafficPeers.trafficPeers.Store(Recipient.String(),traffic)
	//err = trafficService.ReceiveCheque(context.Background(), peerAddress, signedCheque)
	//if !strings.Contains(err.Error(),"account information error") && err != chequePkg.ErrWrongBeneficiary {
	//	t.Fatal(err)
	//}
	//fmt.Println("TestPayUnknownBeneficiary - Success")
}

func TestPay(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(Recipient.String())
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
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return Recipient, nil
		},
		chainID)

	cashOut := cashOutMock{}         //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := addressBookMock{} //NewAddressBook(store)
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
		return Recipient, true, nil
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	trafficService := newTraffic(t, store, logger, p2pStream, chainAddress, transactionService, chequestore, &cashOut, &addressBook, &chequeSigner, chainID)
	trafficService.trafficPeers.balance = new(big.Int).SetInt64(120)
	trafficService.SetNotifyPaymentFunc(func(peer boson.Address, amount *big.Int) error {
		return nil
	})
	err = trafficService.Pay(context.Background(), peerAddress, new(big.Int).SetInt64(100), new(big.Int).SetInt64(80))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("TestPay - Success")
}

func TestPayIssueError(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(Recipient.String())
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
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return Recipient, nil
		},
		chainID)

	cashOut := cashOutMock{}         //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := addressBookMock{} //NewAddressBook(store)
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
		return Recipient, true, nil
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	trafficService := newTraffic(t, store, logger, p2pStream, chainAddress, transactionService, chequestore, &cashOut, &addressBook, &chequeSigner, chainID)
	trafficService.trafficPeers.balance = new(big.Int).SetInt64(0)
	trafficService.SetNotifyPaymentFunc(func(peer boson.Address, amount *big.Int) error {
		return nil
	})
	err = trafficService.Pay(context.Background(), peerAddress, new(big.Int).SetInt64(100), new(big.Int).SetInt64(80))
	if err == nil {
		t.Fatal(err)
	}

	trafficService.trafficPeers.balance = new(big.Int).SetInt64(120)
	err = trafficService.Pay(context.Background(), peerAddress, new(big.Int).SetInt64(100), new(big.Int).SetInt64(80))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("TestPayIssueError - Success")
}

func TestCashOut(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0xab")
	Recipient := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(Recipient.String())
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
	chequeStore := chequePkg.NewChequeStore(
		store,
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return Recipient, nil
		},
		chainID)

	cashOut := cashOutMock{} //chequePkg.NewCashoutService(store, transactionService, chequeStore)
	cashOut.cashCheque = func(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error) {
		return chainAddress.Hash(), nil
	}
	addressBook := addressBookMock{} //NewAddressBook(store)
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool, err error) {
		return Recipient, true, nil
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	trafficService := newTraffic(t, store, logger, p2pStream, chainAddress, transactionService, chequeStore, &cashOut, &addressBook, &chequeSigner, chainID)
	comHash, err := trafficService.CashCheque(context.Background(), peerAddress)
	if err != nil {
		t.Fatal(err)
	}
	if comHash != chainAddress.Hash() {
		t.Fatal("Withdrawal failure")
	}
	fmt.Println("TestCashout - Success")
}

func TestSignCheque(t *testing.T) {
	chequebookAddress := common.HexToAddress("0x8d3766440f0d7b949a5e32995d09619a7f86e632")
	beneficiaryAddress := common.HexToAddress("0xb8d424e9662fe0837fb1d728f1ac97cebb1085fe")
	signature := common.Hex2Bytes("abcd")
	cumulativePayout := big.NewInt(10)
	chainId := int64(1)
	cheque := &chequePkg.Cheque{
		Recipient:        chequebookAddress,
		Beneficiary:      beneficiaryAddress,
		CumulativePayout: cumulativePayout,
	}

	signer := signermock.New(
		signermock.WithSignTypedDataFunc(func(data *eip712.TypedData) ([]byte, error) {

			if data.Message["beneficiary"].(string) != beneficiaryAddress.Hex() {
				t.Fatal("signing cheque with wrong beneficiary")
			}

			if data.Message["recipient"].(string) != chequebookAddress.Hex() {
				t.Fatal("signing cheque for wrong chequebook")
			}

			if data.Message["cumulativePayout"].(string) != cumulativePayout.String() {
				t.Fatal("signing cheque with wrong cumulativePayout")
			}

			return signature, nil
		}),
	)

	chequeSigner := chequePkg.NewChequeSigner(signer, chainId)

	result, err := chequeSigner.Sign(cheque)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(result, signature) {
		t.Fatalf("returned wrong signature. wanted %x, got %x", signature, result)
	}
	fmt.Println("TestPayUnknownBeneficiary - Success")
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

func newTraffic(t *testing.T, store storage.StateStorer, logger logging.Logger, streamer p2p.Streamer, chainAddress common.Address,
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
