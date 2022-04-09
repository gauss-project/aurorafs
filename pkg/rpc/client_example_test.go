package rpc_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gauss-project/aurorafs/pkg/rpc"
)

// In this example, our client wishes to track the latest kadInfo
// known to the server. The server supports the methods
type KadInfo struct {
	Depth      uint8 `json:"depth"`
	Population int   `json:"population"`
}

func ExampleClientSubscription() {
	// Connect the client.
	client, _ := rpc.Dial("ws://127.0.0.1:1637")
	subch := make(chan KadInfo)

	// Ensure that subch receives the latest block.
	go func() {
		subscribeKadInfo(client, subch)
	}()

	// Print events from the subscription as they arrive.
	for block := range subch {
		fmt.Printf("latest depth %d population %d\n", block.Depth, block.Population)
	}
}

// subscribeBlocks runs in its own goroutine and maintains
// a subscription for new blocks.
func subscribeKadInfo(client *rpc.Client, subch chan KadInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe to new blocks.
	sub, err := client.Subscribe(ctx, "p2p", subch, "kadInfo")
	if err != nil {
		fmt.Println("subscribe error:", err)
		return
	}

	// The subscription will deliver events to the channel. Wait for the
	// subscription to end for any reason, then loop around to re-establish
	// the connection.
	fmt.Println("connection lost: ", <-sub.Err())
}
