package mock

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
)

type TrafficAddressBookMock struct {
	beneficiary     func(peer boson.Address) (beneficiary common.Address, known bool, err error)
	beneficiaryPeer func(beneficiary common.Address) (peer boson.Address, known bool, err error)
	putBeneficiary  func(peer boson.Address, beneficiary common.Address) error
	initAddressBook func() error
}

func (m *TrafficAddressBookMock) Beneficiary(peer boson.Address) (beneficiary common.Address, known bool, err error) {
	return m.beneficiary(peer)
}

func (m *TrafficAddressBookMock) BeneficiaryPeer(beneficiary common.Address) (peer boson.Address, known bool, err error) {
	return m.beneficiaryPeer(beneficiary)
}

func (m *TrafficAddressBookMock) PutBeneficiary(peer boson.Address, beneficiary common.Address) error {
	return m.putBeneficiary(peer, beneficiary)
}

func (m *TrafficAddressBookMock) InitAddressBook() error {
	return nil
}

type CashOutMock struct {
	cashCheque func(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error)
}

func (m *CashOutMock) CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error) {
	return m.cashCheque(ctx, chequebook, recipient)
}

type ChequeSignerMock struct {
	sign func(cheque *cheque.Cheque) ([]byte, error)
}

func (m *ChequeSignerMock) Sign(cheque *cheque.Cheque) ([]byte, error) {
	return m.sign(cheque)
}
