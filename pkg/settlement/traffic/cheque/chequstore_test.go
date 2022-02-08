package cheque_test

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"math/big"
	"testing"
)

type chequeSignerMock struct {
	sign func(cheque *chequePkg.Cheque) ([]byte, error)
}

func (m *chequeSignerMock) Sign(cheque *chequePkg.Cheque) ([]byte, error) {
	return m.sign(cheque)
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

	if !errors.Is(err, chequePkg.ErrChequeNotIncreasing) {
		t.Fatal(err)
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
