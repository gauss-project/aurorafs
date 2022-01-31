package accounting

import (
	"bytes"
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	traMock "github.com/gauss-project/aurorafs/pkg/settlement/traffic/mock"
	"github.com/gauss-project/aurorafs/pkg/statestore/mock"
	"io/ioutil"
	"math/big"
	"testing"
	"time"
)

var (
	testPaymentTolerance = big.NewInt(1000)
	testPaymentThreshold = big.NewInt(10000)
)

func TestAccountingReserve(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)
	peer1Addr, err := boson.ParseHexAddress("00112233")
	store := mock.NewStateStore()
	settlement := traMock.NewSettlement(
		traMock.WithRetrieveTraffic(func(peer boson.Address) (traffic *big.Int, err error) {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return new(big.Int).SetInt64(0), nil
		}),
		traMock.WithAvailableBalance(func() (*big.Int, error) {
			return new(big.Int).SetInt64(0), nil
		}),
	)

	defer store.Close()

	acc := NewAccounting(testPaymentThreshold, testPaymentTolerance, logger, store, settlement)

	if err != nil {
		t.Fatal(err)
	}

	// it should allow to cross the threshold one time
	err = acc.Reserve(peer1Addr, testPaymentThreshold.Uint64()+1)
	if err == nil {
		t.Fatal("expected error from reserve")
	}

	if !errors.Is(err, ErrLowAvailableExceeded) {
		t.Fatalf("expected overdraft error from reserve, got %v", err)
	}
}

func TestAccountingCredit(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	peer1Addr, err := boson.ParseHexAddress("00112233")
	chequeSend := false
	settlement := traMock.NewSettlement(
		traMock.WithPutRetrieveTraffic(func(peer boson.Address, traffic *big.Int) error {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return nil
		}),
		traMock.WithRetrieveTraffic(func(peer boson.Address) (traffic *big.Int, err error) {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return big.NewInt(0), nil
		}),
		traMock.WithPay(func(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			if paymentThreshold.Cmp(testPaymentThreshold) != 0 {
				t.Fatal("Wrong threshold value")
			}
			chequeSend = true
			return nil
		}),
		traMock.WithAvailableBalance(func() (*big.Int, error) {
			return new(big.Int).Add(testPaymentThreshold, big.NewInt(100)), nil
		}),
	)

	defer store.Close()

	acc := NewAccounting(testPaymentTolerance, testPaymentThreshold, logger, store, settlement)

	if err != nil {
		t.Fatal(err)
	}

	_ = acc.Credit(context.Background(), peer1Addr, testPaymentThreshold.Uint64())
	err = acc.Reserve(peer1Addr, new(big.Int).Add(testPaymentThreshold, big.NewInt(1)).Uint64())
	if err != nil {
		t.Fatal(err)
	}
	err = acc.Credit(context.Background(), peer1Addr, testPaymentThreshold.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	if !chequeSend {
		t.Fatal("Sending check failed.")
	}
}

func TestAccountingDebit(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)
	store := mock.NewStateStore()
	peer1Addr, err := boson.ParseHexAddress("00112233")

	transferTraffic := new(big.Int).Add(testPaymentTolerance, new(big.Int).SetInt64(1))
	settlement := traMock.NewSettlement(
		traMock.WithTransferTraffic(func(peer boson.Address) (traffic *big.Int, err error) {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return transferTraffic, nil
		}),
		traMock.WithGetPeerBalance(func(peer boson.Address) (*big.Int, error) {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return big.NewInt(0), nil
		}),
		traMock.WithGetUnPaidBalance(func(peer boson.Address) (*big.Int, error) {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return big.NewInt(0), nil
		}),
		traMock.WithPutTransferTraffic(func(peer boson.Address, traffic *big.Int) error {
			if !bytes.Equal(peer.Bytes(), peer1Addr.Bytes()) {
				t.Fatal("Connection address verification failed.")
			}
			return nil
		}),
	)

	defer store.Close()

	acc := NewAccounting(testPaymentTolerance, testPaymentThreshold, logger, store, settlement)

	if err != nil {
		t.Fatal(err)
	}

	err = acc.Debit(peer1Addr, testPaymentThreshold.Uint64())
	if !errors.Is(err, ErrDisconnectThresholdExceeded) {
		t.Fatal("Error in blacklist verification")
	}
	transferTraffic = new(big.Int).Sub(testPaymentTolerance, new(big.Int).SetInt64(1))
	err = acc.Debit(peer1Addr, testPaymentThreshold.Uint64())
	if err != nil {
		t.Fatal("Debit verification failed.")
	}
}

func TestAccountingNotifyPayment(t *testing.T) {
	logger := logging.New(ioutil.Discard, 0)

	store := mock.NewStateStore()
	defer store.Close()
	peer1Addr, err := boson.ParseHexAddress("00112233")
	debtAmount := uint64(100)
	testPayment := new(big.Int).SetInt64(200)
	balance := testPaymentTolerance
	settlement := traMock.NewSettlement(
		traMock.WithAvailableBalance(func() (*big.Int, error) {
			return balance, nil
		}),
		traMock.WithRetrieveTraffic(func(peer boson.Address) (traffic *big.Int, err error) {
			return big.NewInt(0), nil
		}),
	)
	acc := NewAccounting(testPaymentTolerance, testPaymentThreshold, logger, store, settlement)

	if err != nil {
		t.Fatal(err)
	}

	err = acc.Reserve(peer1Addr, testPayment.Uint64())
	if err != nil {
		t.Fatal(err)
	}

	err = acc.NotifyPayment(peer1Addr, new(big.Int).SetUint64(testPayment.Uint64()))
	if err != nil {
		t.Fatal(err)
	}
	balance = new(big.Int).SetUint64(debtAmount)
	err = acc.Reserve(peer1Addr, debtAmount)
	if err != nil {
		t.Fatal(err)
	}

	err = acc.NotifyPayment(peer1Addr, new(big.Int).SetUint64(debtAmount+testPaymentTolerance.Uint64()+1))
	if err != nil { //When the deduction is insufficient, the open check flow becomes zero.
		t.Fatal(err)
	}
	//if !strings.Contains(err.Error(), "unpaid traffic is less than paid traffic") {
	//	t.Fatal("Balance check failed.")
	//}
}
