package traffic

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/subscribe"
	"io"
	"io/ioutil"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/crypto/eip712"
	signermock "github.com/gauss-project/aurorafs/pkg/crypto/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	mockp2p "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	chainTraffic "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic"
	chainTrafficMock "github.com/gauss-project/aurorafs/pkg/settlement/chain/traffic/mock"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	mockchequestore "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque/mock"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/trafficprotocol"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
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

type trafficProtocolMock struct {
	emitCheque func(ctx context.Context, peer boson.Address, cheque *chequePkg.SignedCheque) error
}

func (m *trafficProtocolMock) EmitCheque(ctx context.Context, peer boson.Address, cheque *chequePkg.SignedCheque) error {
	if m.emitCheque != nil {
		return m.emitCheque(ctx, peer, cheque)
	}
	return nil
}

type cashOutMock struct {
	cashCheque     func(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address) (common.Hash, error)
	waitForReceipt func(ctx context.Context, ctxHash common.Hash) (uint64, error)
}

func (m *cashOutMock) CashCheque(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address) (common.Hash, error) {
	return m.cashCheque(ctx, peer, beneficiary, recipient)
}

func (m *cashOutMock) WaitForReceipt(ctx context.Context, ctxHash common.Hash) (uint64, error) {
	return m.waitForReceipt(ctx, ctxHash)
}

type chequeSignerMock struct {
	sign func(cheque *chequePkg.Cheque) ([]byte, error)
}

func (m *chequeSignerMock) Sign(cheque *chequePkg.Cheque) ([]byte, error) {
	return m.sign(cheque)
}

type addressBookMock struct {
	beneficiary     func(peer boson.Address) (beneficiary common.Address, known bool)
	beneficiaryPeer func(beneficiary common.Address) (peer boson.Address, known bool)
	putBeneficiary  func(peer boson.Address, beneficiary common.Address) error
}

func (m *addressBookMock) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool) {
	return m.beneficiary(peer)
}

func (m *addressBookMock) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool) {
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
	Beneficiary := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(Beneficiary.String())
	p2pStream := &trafficProtocolMock{}

	logger := logging.New(ioutil.Discard, 0)
	// transactionService, err := chainTraffic.NewServer(logger, nil, "") //transaction.NewService(logger, nil, Signer, nil, new(big.Int).SetInt64(10))
	transactionService := chainTrafficMock.New(
		chainTrafficMock.WithBalanceOf(func(account common.Address) (*big.Int, error) {
			return new(big.Int).SetInt64(0), nil
		}))

	chequeStore := chequePkg.NewChequeStore(
		store,
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return chainAddress, nil
		},
		chainID)

	cashOut := cashOutMock{}          // chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := &addressBookMock{} // NewAddressBook(store)
	addressBook.putBeneficiary = func(peer boson.Address, beneficiary common.Address) error {
		return nil
	}
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool) {
		return Beneficiary, true
	}

	addressBook.beneficiaryPeer = func(beneficiary common.Address) (peer boson.Address, known bool) {
		return peerAddress, false
	}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	c := chequePkg.Cheque{
		Recipient:        Beneficiary,
		Beneficiary:      chainAddress,
		CumulativePayout: big.NewInt(10),
	}
	sin, err := chequeSigner.Sign(&c)
	if err != nil {
		t.Fatal(err)
	}
	signedCheque := chequePkg.SignedCheque{
		Cheque:    c,
		Signature: sin,
	}

	trafficService := newTrafficTest(t, store, logger, p2pStream, chainAddress, transactionService, chequeStore, &cashOut, addressBook, &chequeSigner, chainID)

	err = trafficService.Handshake(peerAddress, Beneficiary, signedCheque)
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
		beneficiary: func(p boson.Address) (common.Address, bool) {
			if !peer.Equal(p) {
				t.Fatal("querying beneficiary for wrong peer")
			}
			return common.Address{}, false
		},
	}
	chequeSigner := &chequeSignerMock{}
	cashOutMock := &cashOutMock{}

	protocol := &trafficProtocolMock{}

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
		subscribe.NewSubPub(),
	)

	err := trafficService.Pay(context.Background(), peer, big.NewInt(50))
	if !errors.Is(err, ErrUnknownBeneficary) && !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("wrong error. wanted %v, got %v", ErrUnknownBeneficary, err)
	}

	if !disconnectCalled {
		t.Fatal("disconnect was not called")
	}

	fmt.Println("TestPayUnknownBeneficiary - Success")
}

func TestPay(t *testing.T) {

	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	chainAddress := common.HexToAddress("0xab")
	beneficiary := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(chainAddress.String())
	chainID := int64(1)

	peer := boson.MustParseHexAddress("abcd")

	addressBook := &addressBookMock{
		beneficiary: func(p boson.Address) (common.Address, bool) {
			if !peer.Equal(p) {
				t.Fatal("querying beneficiary for wrong peer")
			}
			return beneficiary, true
		},
	}
	cashOut := cashOutMock{}
	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	chequeStore := mockchequestore.NewChequeStore(
		mockchequestore.WithLastSendCheque(func(chainAddress common.Address) (*chequePkg.Cheque, error) {
			return &chequePkg.Cheque{
				CumulativePayout: new(big.Int).SetInt64(0),
			}, nil
		},
		),
		mockchequestore.WithPutSendCheque(func(ctx context.Context, cheque *chequePkg.Cheque, chainAddress common.Address) error {
			return nil
		}),
	)

	var emitCalled bool
	tra := New(
		logger,
		chainAddress,
		store,
		nil,
		chequeStore,
		&cashOut,
		mockp2p.New(),
		addressBook,
		&chequeSigner,
		&trafficProtocolMock{
			emitCheque: func(ctx context.Context, p boson.Address, c *chequePkg.SignedCheque) error {
				if !peer.Equal(p) {
					t.Fatal("sending to wrong peer")
				}
				if c.Cheque.Beneficiary != chainAddress {
					t.Fatal("sending wrong cheque")
				}
				if c.Cheque.Recipient != beneficiary {
					t.Fatal("sending wrong cheque")
				}
				if c.Cheque.CumulativePayout.Cmp(new(big.Int).SetInt64(11)) != 0 {
					t.Fatal("sending wrong cheque")
				}
				emitCalled = true
				return nil
			},
		},
		chainID,
		subscribe.NewSubPub(),
	)
	tra.trafficPeers.balance = new(big.Int).SetInt64(120)
	tra.trafficPeers.trafficPeers[beneficiary.String()] = &Traffic{
		trafficPeerBalance:    big.NewInt(0),
		retrieveChainTraffic:  big.NewInt(0),
		transferChainTraffic:  big.NewInt(0),
		retrieveChequeTraffic: big.NewInt(0),
		transferChequeTraffic: big.NewInt(0),
		retrieveTraffic:       big.NewInt(11),
		transferTraffic:       big.NewInt(0),
	}
	tra.SetNotifyPaymentFunc(func(peer boson.Address, amount *big.Int) error {
		return nil
	})
	err := tra.Pay(context.Background(), peer, new(big.Int).SetInt64(10))
	if err != nil {
		t.Fatal(err)
	}

	if !emitCalled {
		t.Fatal("swap protocol was not called")
	}
	fmt.Println("TestPay - Success")
}

func TestPayIssueError(t *testing.T) {
	store := mock.NewStateStore()
	chainID := int64(1)
	peerAddress := boson.MustParseHexAddress("03")
	chainAddress := common.HexToAddress("0xab")
	beneficiarys := common.HexToAddress("0xcd")
	sig := common.Hex2Bytes(beneficiarys.String())
	p2pStream := &trafficProtocolMock{}

	logger := logging.New(ioutil.Discard, 0)
	transactionService, err := chainTraffic.NewServer(logger, nil, nil, nil, nil, "", nil) // transaction.NewService(logger, nil, Signer, nil, new(big.Int).SetInt64(10))
	if err != nil {
		t.Fatal(err)
	}
	chequeStore := chequePkg.NewChequeStore(
		store,
		chainAddress,
		func(cheque *chequePkg.SignedCheque, chaindID int64) (common.Address, error) {
			return beneficiarys, nil
		},
		chainID)

	cashOut := cashOutMock{}         // chequePkg.NewCashoutService(store, transactionService, chequeStore)
	addressBook := addressBookMock{} // NewAddressBook(store)
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool) {
		return beneficiarys, true
	}

	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}

	trafficService := newTrafficTest(t, store, logger, p2pStream, chainAddress, transactionService, chequeStore, &cashOut, &addressBook, &chequeSigner, chainID)
	trafficService.trafficPeers.balance = new(big.Int).SetInt64(0)
	trafficService.SetNotifyPaymentFunc(func(peer boson.Address, amount *big.Int) error {
		return nil
	})
	trafficService.trafficPeers.trafficPeers[beneficiarys.String()] = &Traffic{
		trafficPeerBalance:    big.NewInt(0),
		retrieveChainTraffic:  big.NewInt(0),
		transferChainTraffic:  big.NewInt(0),
		retrieveChequeTraffic: big.NewInt(0),
		transferChequeTraffic: big.NewInt(0),
		retrieveTraffic:       big.NewInt(81),
		transferTraffic:       big.NewInt(0),
	}
	err = trafficService.Pay(context.Background(), peerAddress, new(big.Int).SetInt64(80))
	if err == nil {
		t.Fatal(err)
	}

	trafficService.trafficPeers.balance = new(big.Int).SetInt64(200)
	err = trafficService.Pay(context.Background(), peerAddress, new(big.Int).SetInt64(80))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("TestPayIssueError - Success")
}

func TestCashOut(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	chainAddress := common.HexToAddress("0xab")

	peer := boson.MustParseHexAddress("abcd")
	txHash := common.HexToHash("eeee")
	addressBook := &addressBookMock{}
	addressBook.beneficiary = func(peer boson.Address) (beneficiary common.Address, known bool) {
		return chainAddress, true
	}
	chainID := int64(1)
	chequeStore := mockchequestore.NewChequeStore()
	cashOut := &cashOutMock{}
	cashOut.cashCheque = func(ctx context.Context, peer boson.Address, beneficiary common.Address, recipient common.Address) (common.Hash, error) {
		return txHash, nil
	}
	chequeSigner := &chequeSignerMock{}
	trafficProtocol := &trafficProtocolMock{}
	tra := New(
		logger,
		chainAddress,
		store,
		nil,
		chequeStore,
		cashOut,
		mockp2p.New(),
		addressBook,
		chequeSigner,
		trafficProtocol,
		chainID,
		subscribe.NewSubPub(),
	)

	returnedHash, err := tra.CashCheque(context.Background(), peer)
	if err != nil {
		t.Fatal(err)
	}

	if returnedHash != txHash {
		t.Fatalf("go wrong tx hash. wanted %v, got %v", txHash, returnedHash)
	}

	fmt.Println("testcashout - Success")
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

func newTrafficTest(t *testing.T, store storage.StateStorer, logger logging.Logger, protocol trafficprotocol.Interface, chainAddress common.Address,
	trafficChainService chain.Traffic, chequeStore chequePkg.ChequeStore, cashout chequePkg.CashoutService,
	addressBook Addressbook, chequeSigner chequePkg.ChequeSigner, chainID int64) *Service {

	// protocol := trafficprotocol.New(streamer, logger)
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
		subscribe.NewSubPub(),
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
