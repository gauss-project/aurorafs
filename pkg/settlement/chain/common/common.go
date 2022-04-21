package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"math/big"
	"strings"
	"sync"
)

type Common struct {
	sync.Mutex
	tx      chain.TxInfo
	logger  logging.Logger
	signer  crypto.Signer
	address *common.Address
	client  *rpc.Client
	chainID *big.Int
}

type TxCommonRequest struct {
	From     string `json:"from"`
	Nonce    string `json:"nonce"`
	To       string `json:"to"`       // recipient of the transaction
	Data     string `json:"data"`     // transaction data
	GasPrice string `json:"gasPrice"` // gas price or nil if suggested gas price should be used
	GasLimit string `json:"gasLimit"` // gas limit or 0 if it should be estimated
	Value    string `json:"value"`    // amount of wei to send
}

func New(logger logging.Logger, signer crypto.Signer, chainId *big.Int, chainEndpoint string) (*Common, error) {
	client, err := rpc.Dial(chainEndpoint)
	if err != nil {
		return nil, err
	}
	address, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	return &Common{logger: logger, signer: signer, address: &address, chainID: chainId, client: client, tx: chain.TxInfo{}}, nil
}

func (c *Common) All(ctx context.Context, req *chain.AllRequest) (*chain.AllResponse, error) {
	var result interface{}
	if req.Method == "eth_sendTransaction" {
		txs := make([]interface{}, len(req.Params))
		req.Method = "eth_sendRawTransaction"
		for i, tx := range req.Params {
			trans, err := c.prepareTransaction(&tx)
			if err != nil {
				return nil, err
			}
			signedTx, err := c.signer.SignTx(trans, c.chainID)
			if err != nil {
				return nil, err
			}
			data, err := signedTx.MarshalBinary()
			if err != nil {
				return nil, err
			}
			encode := hexutil.Encode(data)
			txs[i] = encode
		}
		req.Params = txs
	}

	if req.Method == "eth_accounts" {
		result = []string{c.address.String()}
		resp := &chain.AllResponse{Result: result}
		return resp, nil
	}

	if err := c.client.CallContext(ctx, &result, req.Method, req.Params...); err != nil {
		return nil, err
	}
	resp := &chain.AllResponse{Result: result}

	return resp, nil
}

func (c *Common) prepareTransaction(request *interface{}) (*types.Transaction, error) {

	tx, err := json.Marshal(*request)
	if err != nil {
		return nil, err
	}
	var txRequest TxCommonRequest
	err = json.Unmarshal(tx, &txRequest)
	if err != nil {
		return nil, err
	}

	if txRequest.From != "" && txRequest.From != strings.ToLower(c.address.String()) {
		return nil, fmt.Errorf("incorrect source address for sending transactions")
	}
	to := common.HexToAddress(txRequest.To)
	gasPricer, err := hexutil.DecodeBig(txRequest.GasPrice)
	if err != nil {
		return nil, err
	}
	value, err := hexutil.DecodeBig(txRequest.Value)
	if err != nil {
		return nil, err
	}
	nonce, err := hexutil.DecodeUint64(txRequest.Nonce)
	if err != nil {
		return nil, err
	}
	gasLimit, err := hexutil.DecodeUint64(txRequest.GasLimit)
	if err != nil {
		return nil, err
	}
	return types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    value,
		Gas:      gasLimit,
		GasPrice: gasPricer,
		Data:     common.FromHex(txRequest.Data),
	}), nil
}

var txStatus = false

func (c *Common) SyncTransaction(t chain.TransactionType, value, txHash string) {
	c.Lock()
	defer c.Unlock()
	txStatus = true
	c.tx.Type = t
	c.tx.Value = value
	c.tx.TxHash = txHash
}

func (c *Common) IsTransaction() bool {
	c.Lock()
	defer c.Unlock()
	return txStatus
}

func (c *Common) UpdateStatus(status bool) {
	c.Lock()
	defer c.Unlock()
	if !status {
		c.tx = chain.TxInfo{}
	}
	txStatus = status
}

func (c *Common) GetTransaction() *chain.TxInfo {
	tx := &chain.TxInfo{Type: c.tx.Type, Value: c.tx.Value, TxHash: c.tx.TxHash}
	return tx
}
