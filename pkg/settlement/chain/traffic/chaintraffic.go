package traffic

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
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

func (chainTraffic *ChainTraffic) TransferredAddress(opts *bind.CallOpts, address common.Address, arg1 *big.Int) (common.Address, error) {
	out0, err := chainTraffic.traffic.TransferredAddress(opts, address, arg1)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedAddress(opts *bind.CallOpts, address common.Address, arg1 *big.Int) (common.Address, error) {
	out0, err := chainTraffic.traffic.RetrievedAddress(opts, address, arg1)
	return out0, err
}

func (chainTraffic *ChainTraffic) BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	out0, err := chainTraffic.traffic.BalanceOf(opts, account)
	return out0, err
}

func (chainTraffic *ChainTraffic) RetrievedTotal(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	out0, err := chainTraffic.traffic.RetrievedTotal(opts, arg0)
	return out0, err
}
