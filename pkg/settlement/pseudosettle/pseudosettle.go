package pseudosettle

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	chequePkg "github.com/gauss-project/aurorafs/pkg/settlement/traffic/cheque"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"math/big"
	"strings"
	"sync"
)

var (
	SettlementReceivedPrefix = "pseudosettle_total_received_"
	SettlementSentPrefix     = "pseudosettle_total_sent_"
)

type trafficInfo struct {
	retrieveTraffic *big.Int
	transferTraffic *big.Int
}

type Service struct {
	streamer          p2p.Streamer
	logger            logging.Logger
	store             storage.StateStorer
	notifyPaymentFunc settlement.NotifyPaymentFunc
	metrics           metrics
	trafficMu         sync.RWMutex
	trafficInfo       sync.Map
	address           common.Address
}

func New(streamer p2p.Streamer, logger logging.Logger, store storage.StateStorer, address common.Address) *Service {
	return &Service{
		streamer: streamer,
		logger:   logger,
		metrics:  newMetrics(),
		store:    store,
		address:  address,
	}
}

func newTraffic() *trafficInfo {
	return &trafficInfo{
		retrieveTraffic: big.NewInt(0),
		transferTraffic: big.NewInt(0),
	}
}

func (s *Service) Init() error {

	if err := s.store.Iterate(SettlementReceivedPrefix, func(key, val []byte) (stop bool, err error) {
		addr, err := totalKeyPeer(key, SettlementReceivedPrefix)
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}
		var traffic *big.Int
		if err = s.store.Get(string(key), &traffic); err != nil {
			return true, err
		}
		_, err = s.putRetrieveTraffic(addr, traffic)
		if err != nil {
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}

	if err := s.store.Iterate(SettlementSentPrefix, func(key, val []byte) (stop bool, err error) {
		addr, err := totalKeyPeer(key, SettlementSentPrefix)
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}
		var traffic *big.Int
		if err = s.store.Get(string(key), &traffic); err != nil {
			return true, err
		}
		_, err = s.putTransferTraffic(addr, traffic)
		if err != nil {
			return true, err
		}
		return false, nil
	}); err != nil {
		return err
	}

	return nil
}

func totalKey(peer boson.Address, prefix string) string {
	return fmt.Sprintf("%v%v", prefix, peer.String())
}

func totalKeyPeer(key []byte, prefix string) (peer boson.Address, err error) {
	k := string(key)

	split := strings.SplitAfter(k, prefix)
	if len(split) != 2 {
		return boson.ZeroAddress, errors.New("no peer in key")
	}
	return boson.ParseHexAddress(split[1])
}

// Pay initiates a payment to the given peer
func (s *Service) Pay(ctx context.Context, peer boson.Address, paymentThreshold *big.Int) error {

	return s.notifyPaymentFunc(peer, paymentThreshold)

}

func (s *Service) TransferTraffic(peer boson.Address) (traffic *big.Int, err error) {

	return big.NewInt(0), nil
}

func (s *Service) RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error) {

	return big.NewInt(256 * 1024 * 8), err
}

func (s *Service) PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error {

	retrieveTraffic, err := s.putRetrieveTraffic(peer, traffic)
	if err != nil {
		return err
	}
	return s.store.Put(totalKey(peer, SettlementReceivedPrefix), retrieveTraffic)
}

func (s *Service) putRetrieveTraffic(peer boson.Address, traffic *big.Int) (retrieveTraffic *big.Int, err error) {
	var localTraffic trafficInfo
	s.trafficMu.Lock()
	defer s.trafficMu.Unlock()
	chainTraffic, ok := s.trafficInfo.Load(peer.String())
	if ok {
		localTraffic = chainTraffic.(trafficInfo)
		localTraffic.retrieveTraffic = new(big.Int).Add(localTraffic.retrieveTraffic, traffic)
	} else {
		localTraffic = *newTraffic()
		localTraffic.retrieveTraffic = traffic
	}
	s.trafficInfo.Store(peer.String(), localTraffic)
	return localTraffic.retrieveTraffic, nil
}

func (s *Service) PutTransferTraffic(peer boson.Address, traffic *big.Int) error {

	transferTraffic, err := s.putTransferTraffic(peer, traffic)
	if err != nil {
		return err
	}
	return s.store.Put(totalKey(peer, SettlementSentPrefix), transferTraffic)
}

func (s *Service) putTransferTraffic(peer boson.Address, traffic *big.Int) (transferTraffic *big.Int, err error) {
	s.trafficMu.Lock()
	defer s.trafficMu.Unlock()

	var localTraffic trafficInfo

	chainTraffic, ok := s.trafficInfo.Load(peer.String())
	if ok {
		localTraffic = chainTraffic.(trafficInfo)
		localTraffic.transferTraffic = new(big.Int).Add(localTraffic.transferTraffic, traffic)
	} else {
		localTraffic = *newTraffic()
		localTraffic.transferTraffic = traffic
	}
	s.trafficInfo.Store(peer.String(), localTraffic)

	return localTraffic.transferTraffic, nil
}

// TotalSent returns the total amount sent to a peer
func (s *Service) TotalSent(peer boson.Address) (totalSent *big.Int, err error) {
	key := totalKey(peer, SettlementSentPrefix)
	err = s.store.Get(key, &totalSent)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return totalSent, nil
}

// TotalReceived returns the total amount received from a peer
func (s *Service) TotalReceived(peer boson.Address) (totalReceived *big.Int, err error) {
	key := totalKey(peer, SettlementReceivedPrefix)
	err = s.store.Get(key, &totalReceived)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			//return nil, settlement.ErrPeerNoSettlements
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return totalReceived, nil
}

// SettlementsSent returns all stored sent settlement values for a given type of prefix
func (s *Service) SettlementsSent() (map[string]*big.Int, error) {
	sent := make(map[string]*big.Int)
	err := s.store.Iterate(SettlementSentPrefix, func(key, val []byte) (stop bool, err error) {
		addr, err := totalKeyPeer(key, SettlementSentPrefix)
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}
		if _, ok := sent[addr.String()]; !ok {
			var storevalue *big.Int
			err = s.store.Get(totalKey(addr, SettlementSentPrefix), &storevalue)
			if err != nil {
				return false, fmt.Errorf("get peer %s settlement balance: %w", addr.String(), err)
			}

			sent[addr.String()] = storevalue
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return sent, nil
}

// SettlementsReceived returns all stored received settlement values for a given type of prefix
func (s *Service) SettlementsReceived() (map[string]*big.Int, error) {
	received := make(map[string]*big.Int)
	err := s.store.Iterate(SettlementReceivedPrefix, func(key, val []byte) (stop bool, err error) {
		if !strings.HasPrefix(string(key), SettlementReceivedPrefix) {
			return true, nil
		}

		addr, err := totalKeyPeer(key, SettlementReceivedPrefix)
		if err != nil {
			return false, fmt.Errorf("parse address from key: %s: %w", string(key), err)
		}
		if _, ok := received[addr.String()]; !ok {
			var storevalue *big.Int
			err = s.store.Get(totalKey(addr, SettlementReceivedPrefix), &storevalue)
			if err != nil {
				return false, fmt.Errorf("get peer %s settlement balance: %w", addr.String(), err)
			}

			received[addr.String()] = storevalue
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return received, nil
}

// Balance Get chain balance
func (s *Service) Balance() (*big.Int, error) {
	return new(big.Int).SetUint64(0), nil
}

// AvailableBalance Get actual available balance
func (s *Service) AvailableBalance() (*big.Int, error) {

	return big.NewInt(256 * 1024 * 8 * 4 * 33), nil
}

func (s *Service) UpdatePeerBalance(peer boson.Address) error {

	return nil
}

// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
func (s *Service) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.notifyPaymentFunc = notifyPaymentFunc
}

func (s *Service) GetPeerBalance(peer boson.Address) (*big.Int, error) {

	return big.NewInt(256 * 1024 * 8 * 4 * 33), nil
}

func (s *Service) GetUnPaidBalance(peer boson.Address) (*big.Int, error) {
	return big.NewInt(0), nil
}

func (s *Service) LastSentCheque(peer boson.Address) (*chequePkg.Cheque, error) {
	return nil, nil
}

func (s *Service) LastReceivedCheque(peer boson.Address) (*chequePkg.SignedCheque, error) {
	return nil, nil
}

func (s *Service) CashCheque(ctx context.Context, peer boson.Address) (common.Hash, error) {
	return common.Hash{}, nil
}

func (s *Service) TrafficCheques() ([]*traffic.TrafficCheque, error) {
	var trafficList []*traffic.TrafficCheque
	return trafficList, nil
}

func (s *Service) Address() common.Address {
	return s.address
}

func (s *Service) TrafficInfo() (*traffic.TrafficInfo, error) {
	respTraffic := traffic.NewTrafficInfo()

	s.trafficInfo.Range(func(chainAddress, v interface{}) bool {
		traffic := v.(trafficInfo)
		respTraffic.TotalSendTraffic = new(big.Int).Add(respTraffic.TotalSendTraffic, traffic.transferTraffic)
		respTraffic.ReceivedTraffic = new(big.Int).Add(respTraffic.ReceivedTraffic, traffic.retrieveTraffic)
		return true
	})
	respTraffic.Balance = big.NewInt(0)
	respTraffic.AvailableBalance = big.NewInt(0)

	return respTraffic, nil
}

func (s *Service) TrafficInit() error {
	return nil
}
