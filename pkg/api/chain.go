package api

import (
	"encoding/json"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"io"
	"net/http"
)

type AllRequest struct {
	Id      int           `json:"id"`
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type AllResponse struct {
	Id      int         `json:"id"`
	JsonRpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
}

func (s *server) chainHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if jsonhttp.HandleBodyReadError(err, w) {
			return
		}
		s.logger.Debugf("transaction: read transaction data error: %v", err)
		s.logger.Error("transaction: read transaction data error")
		jsonhttp.InternalServerError(w, "cannot read data")
		return
	}
	var allRequest AllRequest
	if err = json.Unmarshal(body, &allRequest); err != nil {
		s.logger.Debugf("api: all chain handler: unmarshal request body: %v", err)
		s.logger.Error("api: all chain handler: unmarshal request body")
		jsonhttp.BadRequest(w, "Unmarshal json body")
		return
	}

	req := &chain.AllRequest{
		Method: allRequest.Method,
		Params: allRequest.Params,
	}
	resp, err := s.commonChain.All(r.Context(), req)
	if err != nil {
		s.logger.Debugf("api: all chain handler: : %v", err)
		s.logger.Errorf("api: all chain handler: ")
		jsonhttp.BadRequest(w, err.Error())
		return
	}

	jsonhttp.OK(w, AllResponse{
		Id:      allRequest.Id,
		JsonRpc: allRequest.JsonRpc,
		Result:  resp.Result,
	})
}

func (s *server) chainTransactionHandler(w http.ResponseWriter, r *http.Request) {
	tx := s.commonChain.GetTransaction()
	jsonhttp.OK(w, tx)
}
