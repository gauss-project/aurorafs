package api

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gorilla/mux"
	"math/big"
	"net/http"
	"sort"
)

type trafficInfo struct {
	Balance          *big.Int `json:"balance"`
	AvailableBalance *big.Int `json:"availableBalance"`
	TotalSendTraffic *big.Int `json:"totalSendTraffic"`
	ReceivedTraffic  *big.Int `json:"receivedTraffic"`
}

type trafficCheque struct {
	Peer                boson.Address `json:"peer"`
	OutstandingTraffic  *big.Int      `json:"outstandingTraffic"`
	SentSettlements     *big.Int      `json:"sentSettlements"`
	ReceivedSettlements *big.Int      `json:"receivedSettlements"`
	Total               *big.Int      `json:"total"`
	UnCashed            *big.Int      `json:"unCashed"`
}

func (s *server) trafficInfo(w http.ResponseWriter, r *http.Request) {
	tra, err := s.traffic.TrafficInfo()
	if err != nil {
		s.logger.Error("Api-trafficInfo Failed to get traffic information: %v", err)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	var traffic trafficInfo
	traffic.Balance = tra.Balance
	traffic.AvailableBalance = tra.AvailableBalance
	traffic.TotalSendTraffic = tra.TotalSendTraffic
	traffic.ReceivedTraffic = tra.ReceivedTraffic
	jsonhttp.OK(w, traffic)
}

func (s *server) address(w http.ResponseWriter, r *http.Request) {
	address := s.traffic.Address()
	jsonhttp.OK(w, struct {
		References common.Address `json:"references"`
	}{
		References: address,
	})
}

func (s *server) trafficCheques(w http.ResponseWriter, r *http.Request) {
	var chequeList []*trafficCheque
	list, err := s.traffic.TrafficCheques()
	if err != nil {
		s.logger.Error("Api trafficInfo: Failed to get traffic information: %v", err)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	for _, v := range list {
		cheque := &trafficCheque{
			Peer:                v.Peer,
			OutstandingTraffic:  v.OutstandingTraffic,
			SentSettlements:     v.SentSettlements,
			ReceivedSettlements: v.ReceivedSettlements,
			Total:               v.Total,
			UnCashed:            v.Uncashed,
		}
		chequeList = append(chequeList, cheque)
	}

	sort.Slice(chequeList, func(i, j int) bool {
		return chequeList[i].Total.Cmp(chequeList[j].Total) < 0
	})
	jsonhttp.OK(w, chequeList)
}

func (s *server) cashCheque(w http.ResponseWriter, r *http.Request) {
	nameOrHex := mux.Vars(r)["address"]
	peer, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		s.logger.Errorf("api cashCheque: parse address %s: %v", nameOrHex, err)
		jsonhttp.NotFound(w, err)
		return
	}
	hash, err := s.traffic.CashCheque(context.Background(), peer)
	if err != nil {
		s.logger.Errorf("api cashCheque: query failed %s: %v", nameOrHex, err)
		jsonhttp.NotFound(w, err)
		return
	}

	type out struct {
		Hash common.Hash `json:"hash"`
	}
	jsonhttp.OK(w, out{Hash: hash})
}
