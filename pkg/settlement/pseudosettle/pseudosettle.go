package pseudosettle

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/settlement"
	pb "github.com/gauss-project/aurorafs/pkg/settlement/pseudosettle/pb"
	"github.com/gauss-project/aurorafs/pkg/storage"
)

const (
	protocolName    = "pseudosettle"
	protocolVersion = "1.0.0"
	streamName      = "traffic"
)

var (
	SettlementReceivedPrefix = "pseudosettle_total_received_"
	SettlementSentPrefix     = "pseudosettle_total_sent_"
)

type Service struct {
	streamer          p2p.Streamer
	logger            logging.Logger
	store             storage.StateStorer
	notifyPaymentFunc settlement.NotifyPaymentFunc
	metrics           metrics
}

func New(streamer p2p.Streamer, logger logging.Logger, store storage.StateStorer) *Service {
	return &Service{
		streamer: streamer,
		logger:   logger,
		metrics:  newMetrics(),
		store:    store,
	}
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
	}
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

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	var req pb.Payment
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}

	s.metrics.TotalReceivedPseudoSettlements.Add(float64(req.Amount))
	s.logger.Tracef("received payment message from peer %v of %d", p.Address, req.Amount)

	totalReceived, err := s.TotalReceived(p.Address)
	if err != nil {
		if !errors.Is(err, settlement.ErrPeerNoSettlements) {
			return err
		}
		totalReceived = big.NewInt(0)
	}

	err = s.store.Put(totalKey(p.Address, SettlementReceivedPrefix), totalReceived.Add(totalReceived, new(big.Int).SetUint64(req.Amount)))
	if err != nil {
		return err
	}

	return s.notifyPaymentFunc(p.Address, new(big.Int).SetUint64(req.Amount))
}

// Pay initiates a payment to the given peer
func (s *Service) Pay(ctx context.Context, peer boson.Address, traffic, paymentThreshold *big.Int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	s.logger.Tracef("sending payment message to peer %v of %d", peer, traffic)
	w := protobuf.NewWriter(stream)
	err = w.WriteMsgWithContext(ctx, &pb.Payment{
		Amount: traffic.Uint64(),
	})
	if err != nil {
		return err
	}
	totalSent, err := s.TotalSent(peer)
	if err != nil {
		if !errors.Is(err, settlement.ErrPeerNoSettlements) {
			return err
		}
		totalSent = big.NewInt(0)
	}
	err = s.store.Put(totalKey(peer, SettlementSentPrefix), totalSent.Add(totalSent, traffic))
	if err != nil {
		return err
	}

	amountFloat, _ := new(big.Float).SetInt(traffic).Float64()
	s.metrics.TotalSentPseudoSettlements.Add(amountFloat)
	return nil
}

func (s *Service) TransferTraffic(peer boson.Address) (traffic *big.Int, err error) {

	err = s.store.Get(totalKey(peer, SettlementSentPrefix), &traffic)
	return traffic, err
}

func (s *Service) RetrieveTraffic(peer boson.Address) (traffic *big.Int, err error) {
	err = s.store.Get(totalKey(peer, SettlementReceivedPrefix), &traffic)
	return traffic, err
}

func (s *Service) PutRetrieveTraffic(peer boson.Address, traffic *big.Int) error {
	return s.store.Put(totalKey(peer, SettlementReceivedPrefix), traffic)
}

func (s *Service) PutTransferTraffic(peer boson.Address, traffic *big.Int) error {
	return s.store.Put(totalKey(peer, SettlementSentPrefix), traffic)
}

// TotalSent returns the total amount sent to a peer
func (s *Service) TotalSent(peer boson.Address) (totalSent *big.Int, err error) {
	key := totalKey(peer, SettlementSentPrefix)
	err = s.store.Get(key, &totalSent)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, settlement.ErrPeerNoSettlements
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
			return nil, settlement.ErrPeerNoSettlements
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
	//s.store.Put(totalKey(peer, SettlementReceivedPrefix), traffic)
	var availableBalance *big.Int
	availableBalance = new(big.Int).SetInt64(0)
	s.store.Iterate(SettlementReceivedPrefix, func(key, value []byte) (stop bool, err error) {
		balance := new(big.Int).SetInt64(0)
		keys := string(key)
		if err := s.store.Get(keys, &balance); err != nil {
			return true, err
		}
		availableBalance = new(big.Int).Add(availableBalance, balance)
		return false, nil
	})

	return availableBalance, nil
}

func (s *Service) UpdatePeerBalance(peer boson.Address) error {

	return nil
}

// SetNotifyPaymentFunc sets the NotifyPaymentFunc to notify
func (s *Service) SetNotifyPaymentFunc(notifyPaymentFunc settlement.NotifyPaymentFunc) {
	s.notifyPaymentFunc = notifyPaymentFunc
}

func (s *Service) GetPeerBalance(peer boson.Address) (*big.Int, error) {
	receivedKey := totalKey(peer, SettlementReceivedPrefix)
	sendKey := totalKey(peer, SettlementSentPrefix)

	var balance, receivedBalance, sendBalance *big.Int
	err := s.store.Get(receivedKey, &receivedBalance)
	if err != nil {
		return balance, err
	}
	err = s.store.Get(sendKey, &sendBalance)
	if err != nil {
		return balance, err
	}

	balance = big.NewInt(0).Sub(receivedBalance, sendBalance)

	return balance, nil
}

func (s *Service) GetUnPaidBalance(peer boson.Address) (*big.Int, error) {
	return big.NewInt(0), nil
}
