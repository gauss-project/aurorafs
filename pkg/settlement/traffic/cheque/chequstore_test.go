package cheque_test

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/addressbook"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/libp2p"
	mockp2p "github.com/gauss-project/aurorafs/pkg/p2p/mock"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	mockchequestore "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque/mock"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"github.com/gauss-project/aurorafs/pkg/topology/bootnode"
	"github.com/gauss-project/aurorafs/pkg/topology/lightnode"
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

// TestReceiveCheque testing receive cheque
func TestReceiveCheque(t *testing.T) {
	ctx := context.Background()
	store := mock.NewStateStore()
	recipient := common.HexToAddress("0xffff")
	chainAddress := common.HexToAddress("0xab")
	issuer := common.HexToAddress("0xbeee")
	cumulativePayout := big.NewInt(10)
	//	cumulativePayout2 := big.NewInt(20)

	chainID := int64(1)
	sig := common.Hex2Bytes(issuer.String())
	cheque := chequePkg.Cheque{
		Beneficiary:      chainAddress,
		Recipient:        recipient,
		CumulativePayout: cumulativePayout,
	}

	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err := chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}
	signCheque := chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}

	chequeStore := chequePkg.NewChequeStore(store, recipient, func(cheque *chequePkg.SignedCheque, cid int64) (common.Address, error) {
		if cid != chainID {
			t.Fatalf("recovery with wrong chain id. wanted %d, got %d", chainID, cid)
		}
		if !cheque.Equal(cheque) {
			t.Fatalf("recovery with wrong cheque. wanted %v, got %v", cheque, store)
		}
		return chainAddress, nil
	}, chainID)
	amount, err := chequeStore.ReceiveCheque(ctx, &signCheque)
	// 	amount, err := chequeStore.ReceiveCheque(ctx, &signCheque)
	if err != nil {
		t.Fatalf(" Failed error %v", err)
	}

	if amount.Cmp(cumulativePayout) != 0 {
		t.Fatalf("calculated wrong received cumulativePayout. wanted %d, got %d", cumulativePayout, amount)
	}

	if amount == nil {
		t.Fatalf(" Failed error  %v", amount)
	}

	if amount.Cmp(cumulativePayout) != 0 {
		t.Fatalf("calculated wrong received cumulativePayout. wanted %d, got %d", cumulativePayout, amount)
	}
	println("received cheque, testing pass")
}

func TestReceiveChequeInvalidBeneficiary(t *testing.T) {
	store := mock.NewStateStore()
	beneficiary := common.HexToAddress("0xffff")
	issuer := common.HexToAddress("0xbeee")
	cumulativePayout := big.NewInt(10)
	//cumulativePayoutLower := big.NewInt(5)
	sig := make([]byte, 65)
	chainID := int64(1)

	cheque := chequePkg.Cheque{
		Beneficiary:      beneficiary,
		Recipient:        issuer,
		CumulativePayout: cumulativePayout,
	}

	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err := chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}

	signCheque := chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}
	chequeStore := chequePkg.NewChequeStore(store, beneficiary, nil, chainID)

	_, err = chequeStore.ReceiveCheque(context.Background(), &signCheque)
	if err == nil {
		t.Fatal("testing pass! accepted cheque with wrong beneficiary")
	}
	println(" testing pass")
}

func TestReceiveChequeInvalidTraffic(t *testing.T) {

	store := mock.NewStateStore()
	beneficiary := common.HexToAddress("0xffff")
	issuer := common.HexToAddress("0xffff")
	cumulativePayout := big.NewInt(10)
	cumulativePayoutLower := big.NewInt(5)
	sig := make([]byte, 65)
	chainID := int64(1)

	cheque := chequePkg.Cheque{
		Beneficiary:      beneficiary,
		Recipient:        issuer,
		CumulativePayout: cumulativePayout,
	}

	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err := chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}

	signCheque := chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}

	chequeStore := chequePkg.NewChequeStore(store, beneficiary, func(cheque *chequePkg.SignedCheque, cid int64) (common.Address, error) {
		if cid != chainID {
			t.Fatalf("recovery with wrong chain id. wanted %d, got %d", chainID, cid)
		}
		if !cheque.Equal(cheque) {
			t.Fatalf("recovery with wrong cheque. wanted %v, got %v", cheque, store)
		}
		return beneficiary, nil
	}, chainID)

	_, err = chequeStore.ReceiveCheque(context.Background(), &signCheque)
	if err != nil {
		t.Fatalf("wrong error. %v", err)
	}

	cheque = chequePkg.Cheque{
		Beneficiary:      beneficiary,
		Recipient:        issuer,
		CumulativePayout: cumulativePayoutLower,
	}
	chequeSigner = chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err = chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}

	signCheque = chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}

	chequeStore = chequePkg.NewChequeStore(store, beneficiary, func(cheque *chequePkg.SignedCheque, cid int64) (common.Address, error) {
		if cid != chainID {
			t.Fatalf("recovery with wrong chain id. wanted %d, got %d", chainID, cid)
		}
		if !cheque.Equal(cheque) {
			t.Fatalf("recovery with wrong cheque. wanted %v, got %v", cheque, store)
		}
		return beneficiary, nil
	}, chainID)

	_, err = chequeStore.ReceiveCheque(context.Background(), &signCheque)
	if err == nil {
		t.Fatalf("wrong error. %v", err)
	}

	println(" testing pass")
}

func TestReceiveChequeInvalidSignature(t *testing.T) {

	store := mock.NewStateStore()
	recipient := common.HexToAddress("0xffff")
	chainAddress := common.HexToAddress("0xab")
	issuer := common.HexToAddress("0xbeee")
	cumulativePayout := big.NewInt(10)

	chainID := int64(1)
	sig := common.Hex2Bytes(issuer.String())

	cheque := chequePkg.Cheque{
		Beneficiary:      chainAddress,
		Recipient:        recipient,
		CumulativePayout: cumulativePayout,
	}

	chequeSigner := chequeSignerMock{}
	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err := chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}
	signCheque := chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}

	chequestore := chequePkg.NewChequeStore(store, recipient, func(cheque *chequePkg.SignedCheque, cid int64) (common.Address, error) {
		return common.Address{}, nil
	}, chainID)
	_, err = chequestore.ReceiveCheque(context.Background(), &signCheque)
	if !errors.Is(err, chequePkg.ErrChequeInvalid) {
		t.Fatalf("wrong error. wanted %v, got %v", chequePkg.ErrChequeInvalid, err)
	}
}

func TestReceiveChequeInsufficientBalance(t *testing.T) {
	ctx := context.Background()
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	peer := boson.MustParseHexAddress("0bee")
	chainAddress := common.HexToAddress("0xaab")

	recipient := common.HexToAddress("0xffff")

	issuer := common.HexToAddress("0xbeee")
	cumulativePayout := big.NewInt(10)

	chainID := int64(1)

	addressBook := &addressBookMock{
		beneficiary: func(p boson.Address) (common.Address, bool, error) {
			if !peer.Equal(p) {
				t.Fatal("querying beneficiary for wrong peer")
			}
			return recipient, true, nil
		},
	}
	chequeSigner := &chequeSignerMock{}
	cashOutMock := &cashOutMock{}

	protocol := &trafficProtocolMock{}

	var disconnectCalled bool
	trafficService := traffic.New(
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
	fmt.Println(disconnectCalled)
	sig := common.Hex2Bytes(issuer.String())
	cheque := chequePkg.Cheque{
		Beneficiary:      recipient,
		Recipient:        chainAddress,
		CumulativePayout: cumulativePayout,
	}

	chequeSigner.sign = func(cheque *chequePkg.Cheque) ([]byte, error) {
		return sig, nil
	}
	sin, err := chequeSigner.Sign(&cheque)
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}
	signCheque := chequePkg.SignedCheque{
		Cheque:    cheque,
		Signature: sin,
	}

	err = trafficService.ReceiveCheque(ctx, peer, &signCheque)
	if err != nil {
		t.Fatalf("wrong error %v", err)
	}

}
