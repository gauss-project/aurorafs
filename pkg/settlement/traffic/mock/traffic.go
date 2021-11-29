package mock

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
)

type trafficAddressBookMock struct {
	beneficiary     func(peer boson.Address) (beneficiary common.Address, known bool, err error)
	beneficiaryPeer func(beneficiary common.Address) (peer boson.Address, known bool, err error)
	putBeneficiary  func(peer boson.Address, beneficiary common.Address) error
	initAddressBook func() error
}

func (m *trafficAddressBookMock) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	return m.beneficiary(peer)
}

func (m *trafficAddressBookMock) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	return m.beneficiaryPeer(beneficiary)
}

func (m *trafficAddressBookMock) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	return m.putBeneficiary(peer, beneficiary)
}

func (m *trafficAddressBookMock) InitAddressBook() error {
	return nil
}

type cashOutMock struct {
	cashCheque func(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error)
}

func (m *cashOutMock) CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error) {
	return m.cashCheque(ctx, chequebook, recipient)
}

type chequeSignerMock struct {
	sign func(cheque *cheque.Cheque) ([]byte, error)
}

func (m *chequeSignerMock) Sign(cheque *cheque.Cheque) ([]byte, error) {
	return m.sign(cheque)
}

//
//type ChequeStore interface {
//	// ReceiveCheque verifies and stores a cheque. It returns the totam amount earned.
//	ReceiveCheque(ctx context.Context, cheque *SignedCheque) (*big.Int, error)
//	// LastCheque returns the last cheque we received from a specific chequebook.
//	LastCheque(chequebook common.Address) (*SignedCheque, error)
//	// LastCheques returns the last received cheques from every known chequebook.
//	LastCheques() (map[common.Address]*SignedCheque, error)
//}

//func (s *Service) ReceiveCheque(ctx context.Context, cheque *chequebook.SignedCheque) (*big.Int, error) {
//	return s.receiveCheque(ctx, cheque)
//}
//
//func (s *Service) LastCheque(chequebook common.Address) (*chequebook.SignedCheque, error) {
//	return s.lastCheque(chequebook)
//}
//
//func (s *Service) LastCheques() (map[common.Address]*chequebook.SignedCheque, error) {
//	return s.lastCheques()
//}
