package debugapi

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
)

type TxRequest struct {
	To       string `json:"to"`       // recipient of the transaction
	Data     string `json:"data"`     // transaction data
	GasLimit uint64 `json:"gasLimit"` // gas limit or 0 if it should be estimated
}

type txResponse struct {
	TxHash string `json:"txHash"`
}

func (s *Service) privateKeyHandler(w http.ResponseWriter, r *http.Request) {
	pk := crypto.EncodeSecp256k1PrivateKey(s.nodeOptions.PrivateKey)

	type out struct {
		PrivateKey string `json:"private_key"`
	}
	jsonhttp.OK(w, out{PrivateKey: fmt.Sprintf("%x", pk)})
}

func (s *Service) transactionHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if jsonhttp.HandleBodyReadError(err, w) {
			return
		}
		s.logger.Debugf("transaction: read transaction data error: %v", err)
		s.logger.Error("transaction: read transaction data error")
		jsonhttp.InternalServerError(w, "cannot read transaction data")
		return
	}
	var txRequest TxRequest
	if err = json.Unmarshal(body, &txRequest); err != nil {
		s.logger.Debugf("api: transaction handler: unmarshal request body: %v", err)
		s.logger.Error("api: transaction handler: unmarshal request body")
		jsonhttp.BadRequest(w, "Unmarshal json body")
		return
	}
	to := common.HexToAddress(txRequest.To)
	gasLimit := txRequest.GasLimit
	if gasLimit == 0 {
		gasLimit = 1000000
	}
	tx := chain.TxRequest{
		To:       &to,
		Data:     common.FromHex(txRequest.Data),
		GasLimit: txRequest.GasLimit,
		Value:    big.NewInt(0),
	}

	txHash, err := s.transaction.Send(r.Context(), &tx)
	if err != nil {
		s.logger.Debugf("api: transaction handler: transaction: %v", err)
		s.logger.Errorf("api: transaction handler: transaction")
		jsonhttp.BadRequest(w, err.Error())
		return
	}

	jsonhttp.OK(w, txResponse{
		TxHash: txHash.String(),
	})
}
