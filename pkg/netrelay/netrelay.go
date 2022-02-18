package netrelay

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"
)

type NetRelay interface {
	RelayHttpDo(msg interface{}) error
}
type Service struct {
	streamer p2p.Streamer
	logging  logging.Logger
	address  common.Address
}

func New(streamer p2p.Streamer, logging logging.Logger, address common.Address) *Service {
	return &Service{streamer: streamer, logging: logging, address: address}
}

func (s *Service) RelayHttpDo(msg interface{}) error {

}
