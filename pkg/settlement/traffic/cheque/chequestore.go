package cheque

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

var (
	// ErrNoCheque is the error returned if there is no prior cheque for a chainAddress or beneficiary.
	ErrNoCheque = errors.New("no cheque")
	// ErrChequeNotIncreasing is the error returned if the cheque amount is the same or lower.
	ErrChequeNotIncreasing = errors.New("cheque cumulativePayout is not increasing")
	// ErrChequeInvalid is the error returned if the cheque itself is invalid.
	ErrChequeInvalid = errors.New("invalid cheque")
	// ErrWrongBeneficiary is the error returned if the cheque has the wrong beneficiary.
	ErrWrongBeneficiary = errors.New("wrong beneficiary")
	// ErrBouncingCheque is the error returned if the chainAddress is demonstrably illiquid.
	ErrBouncingCheque        = errors.New("bouncing cheque")
	lastReceivedChequePrefix = "traffic_last_received_cheque_"
	lastSendChequePrefix     = "traffic_last_send_cheque_"
	retrievedTrafficPrefix   = "retrieved_traffic_"
	transferredTrafficPrefix = "transferred_traffic_"
)

// ChequeStore handles the verification and storage of received cheques
type ChequeStore interface {
	// ReceiveCheque verifies and stores a cheque. It returns the totam amount earned.
	ReceiveCheque(ctx context.Context, cheque *SignedCheque) (*big.Int, error)

	PutSendCheque(ctx context.Context, cheque *Cheque, chainAddress common.Address) error
	// LastReceivedCheque returns the last cheque we received from a specific chainAddress.
	LastReceivedCheque(chainAddress common.Address) (*SignedCheque, error)
	// LastReceivedCheques returns the last received cheques from every known chainAddress.
	LastReceivedCheques() (map[common.Address]*SignedCheque, error)

	LastSendCheque(chainAddress common.Address) (*Cheque, error)

	LastSendCheques() (map[common.Address]*Cheque, error)

	PutRetrieveTraffic(chainAddress common.Address, traffic *big.Int) error

	PutTransferTraffic(chainAddress common.Address, traffic *big.Int) error

	PutReceivedCheques(chainAddress common.Address, cheque SignedCheque) error

	VerifyCheque(cheque *SignedCheque, chaindID int64) (common.Address, error)
}

type chequeStore struct {
	lock              sync.Mutex
	store             storage.StateStorer
	recipient         common.Address // the beneficiary we expect in cheques sent to us
	recoverChequeFunc RecoverChequeFunc
	chainID           int64
}

type RecoverChequeFunc func(cheque *SignedCheque, chainID int64) (common.Address, error)

// NewChequeStore creates new ChequeStore
func NewChequeStore(
	store storage.StateStorer,
	recipient common.Address,
	recoverChequeFunc RecoverChequeFunc,
	chainID int64) ChequeStore {
	return &chequeStore{
		store:             store,
		recipient:         recipient,
		recoverChequeFunc: recoverChequeFunc,
		chainID:           chainID,
	}
}

// lastTransferredTrafficChequeKey computes the key where to store the last cheque received from a chainAddress.
func lastReceivedChequeKey(chainAddress common.Address) string {
	return fmt.Sprintf("%s_%x", lastReceivedChequePrefix, chainAddress)
}

func lastSendChequeKey(chainAddress common.Address) string {
	return fmt.Sprintf("%s_%x", lastSendChequePrefix, chainAddress)
}

func retrievedTraffic(chainAddress common.Address) string {
	return fmt.Sprintf("%s_%x", retrievedTrafficPrefix, chainAddress)
}

func transferredTraffic(chainAddress common.Address) string {
	return fmt.Sprintf("%s_%x", transferredTrafficPrefix, chainAddress)
}

// LastReceivedCheque returns the last cheque we received from a specific chainAddress.
func (s *chequeStore) LastReceivedCheque(chainAddress common.Address) (*SignedCheque, error) {
	var cheque *SignedCheque
	err := s.store.Get(lastReceivedChequeKey(chainAddress), &cheque)
	if err != nil {
		if err != storage.ErrNotFound {
			return nil, err
		}
		return &SignedCheque{
			Cheque: Cheque{
				Recipient:        common.Address{},
				Beneficiary:      common.Address{},
				CumulativePayout: big.NewInt(0),
			},
		}, ErrNoCheque
	}

	return cheque, nil
}

// ReceiveCheque verifies and stores a cheque. It returns the totam amount earned.
func (s *chequeStore) ReceiveCheque(ctx context.Context, cheque *SignedCheque) (*big.Int, error) {
	// verify we are the beneficiary
	if cheque.Recipient != s.recipient {
		return nil, ErrWrongBeneficiary
	}

	// verify the cheque signature
	issuer, err := s.recoverChequeFunc(cheque, s.chainID)
	if err != nil {
		return nil, err
	}

	if issuer != cheque.Beneficiary {
		return nil, ErrChequeInvalid
	}

	// don't allow concurrent processing of cheques
	// this would be sufficient on a per chainAddress basis
	s.lock.Lock()
	defer s.lock.Unlock()

	// load the lastCumulativePayout for the cheques chainAddress
	var lastCumulativePayout *big.Int
	var lastReceivedCheque *SignedCheque
	err = s.store.Get(lastReceivedChequeKey(cheque.Beneficiary), &lastReceivedCheque)
	if err != nil {
		if err != storage.ErrNotFound {
			return nil, err
		}

		lastCumulativePayout = big.NewInt(0)
	} else {
		lastCumulativePayout = lastReceivedCheque.CumulativePayout
	}

	// check this cheque is actually increasing in value
	amount := big.NewInt(0).Sub(cheque.CumulativePayout, lastCumulativePayout)

	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil, ErrChequeNotIncreasing
	}

	// store the accepted cheque
	err = s.store.Put(lastReceivedChequeKey(cheque.Beneficiary), cheque)
	if err != nil {
		return nil, err
	}
	return amount, nil
}

func (s *chequeStore) PutSendCheque(ctx context.Context, cheque *Cheque, chainAddress common.Address) error {
	return s.store.Put(lastSendChequeKey(chainAddress), cheque)
}

// RecoverCheque recovers the issuer ethereum address from a signed cheque
func RecoverCheque(cheque *SignedCheque, chaindID int64) (common.Address, error) {
	eip712Data := eip712DataForCheque(&cheque.Cheque, chaindID)

	pubkey, err := crypto.RecoverEIP712(cheque.Signature, eip712Data)
	if err != nil {
		return common.Address{}, err
	}

	ethAddr, err := crypto.NewEthereumAddress(*pubkey)
	if err != nil {
		return common.Address{}, err
	}

	var issuer common.Address
	copy(issuer[:], ethAddr)
	return issuer, nil
}

// keyChequebook computes the chainAddress a store entry is for.
func keyChainAddress(key []byte, prefix string) (chainAddress common.Address, err error) {
	k := string(key)

	split := strings.SplitAfter(k, prefix)
	if len(split) != 2 {
		return common.Address{}, errors.New("no peer in key")
	}
	return common.HexToAddress(split[1]), nil
}

// LastReceivedCheques returns the last received cheques from every known chainAddress.
func (s *chequeStore) LastReceivedCheques() (map[common.Address]*SignedCheque, error) {
	result := make(map[common.Address]*SignedCheque)
	err := s.store.Iterate(lastReceivedChequePrefix, func(key, val []byte) (stop bool, err error) {
		addr, err := keyChainAddress(key, lastReceivedChequePrefix+"_")
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}

		if _, ok := result[addr]; !ok {
			lastCheque, err := s.LastReceivedCheque(addr)
			if err != nil && err != ErrNoCheque {
				return false, err
			} else if err == ErrNoCheque {
				return false, nil
			}

			result[addr] = lastCheque
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *chequeStore) LastSendCheque(chainAddress common.Address) (*Cheque, error) {
	var cheque *Cheque
	err := s.store.Get(lastSendChequeKey(chainAddress), &cheque)
	if err != nil {
		if err != storage.ErrNotFound {
			return nil, err
		}
		return &Cheque{
			Recipient:        common.Address{},
			Beneficiary:      common.Address{},
			CumulativePayout: new(big.Int).SetInt64(0),
		}, ErrNoCheque
	}

	return cheque, nil
}

func (s *chequeStore) LastSendCheques() (map[common.Address]*Cheque, error) {
	result := make(map[common.Address]*Cheque)
	err := s.store.Iterate(lastSendChequePrefix, func(key, val []byte) (stop bool, err error) {
		addr, err := keyChainAddress(key, lastSendChequePrefix+"_")
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}

		if _, ok := result[addr]; !ok {
			lastCheque, err := s.LastSendCheque(addr)
			if err != nil && err != ErrNoCheque {
				return false, err
			} else if err == ErrNoCheque {
				return false, nil
			}

			result[addr] = lastCheque
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *chequeStore) PutRetrieveTraffic(chainAddress common.Address, traffic *big.Int) error {
	return s.store.Put(retrievedTraffic(chainAddress), traffic)
}

func (s *chequeStore) PutTransferTraffic(chainAddress common.Address, traffic *big.Int) error {
	return s.store.Put(transferredTraffic(chainAddress), traffic)
}

func (s *chequeStore) PutReceivedCheques(chainAddress common.Address, cheque SignedCheque) error {
	return s.store.Put(lastReceivedChequeKey(chainAddress), cheque)
}

// RecoverCheque recovers the issuer ethereum address from a signed cheque
func (s *chequeStore) RecoverCheque(cheque *SignedCheque, chainID int64) (common.Address, error) {
	eip712Data := eip712DataForCheque(&cheque.Cheque, chainID)

	pubkey, err := crypto.RecoverEIP712(cheque.Signature, eip712Data)
	if err != nil {
		return common.Address{}, err
	}

	ethAddr, err := crypto.NewEthereumAddress(*pubkey)
	if err != nil {
		return common.Address{}, err
	}

	var issuer common.Address
	copy(issuer[:], ethAddr)
	return issuer, nil
}

func (s *chequeStore) VerifyCheque(cheque *SignedCheque, chaindID int64) (common.Address, error) {
	return s.recoverChequeFunc(cheque, chaindID)
}
