package traffic

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
)

type ChainTraffic struct {
	logger  logging.Logger
	traffic *Traffic
}

func NewServer(logger logging.Logger, backend *ethclient.Client, address string) (chain.Traffic, error) {

	traffic, err := NewTraffic(common.HexToAddress(address), backend)
	if err != nil {
		logger.Errorf("Failed to connect to the Ethereum client: %v", err)
		return &ChainTraffic{}, err
	}
	return &ChainTraffic{
		logger:  logger,
		traffic: traffic,
	}, nil
}

func (chainTraffic *ChainTraffic) TransferredAddress(address common.Address) ([]common.Address, error) {
	out0, err := chainTraffic.traffic.GetTransferredAddress(nil, address)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedAddress(address common.Address) ([]common.Address, error) {
	out0, err := chainTraffic.traffic.GetRetrievedAddress(nil, address)
	return out0, err
}

func (chainTraffic *ChainTraffic) BalanceOf(account common.Address) (*big.Int, error) {
	out0, err := chainTraffic.traffic.BalanceOf(nil, account)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedTotal(arg0 common.Address) (*big.Int, error) {
	out0, err := chainTraffic.traffic.RetrievedTotal(nil, arg0)
	return out0, err
}

func (chainTraffic *ChainTraffic) TransferredTotal(arg0 common.Address) (*big.Int, error) {
	out0, err := chainTraffic.traffic.TransferredTotal(nil, arg0)
	return out0, err
}

func (chainTraffic *ChainTraffic) FilterTransfer(from common.Address, to common.Address) (*big.Int, error) {
	return chainTraffic.traffic.TransAmount(nil, from, to)
}
