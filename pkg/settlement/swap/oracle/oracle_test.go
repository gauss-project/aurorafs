package oracle

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"testing"
)

func TestOracle(t *testing.T) {

	ctx := context.Background()
	conn, err := ethclient.DialContext(ctx, "https://data-seed-prebsc-1-s1.binance.org:8545/")
	if err != nil {
		t.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}

	oracle, err := NewOracle(common.HexToAddress("0x941d06f88ff13f78aecb8b6906159fe9f205eddc"), conn)

	if err != nil {
		t.Fatalf("Failed error %v", err)
	}

	v, err := oracle.Get(nil, common.HexToHash("f44a7f8ed52047c74b938fb5d1492bdc4f805bfa5db15ba0a44d3f017146ef78"))
	if err != nil {
		t.Fatalf("Failed error %v", err)
	}
	fmt.Println(common.Bytes2Hex(v[0][:]))
}
