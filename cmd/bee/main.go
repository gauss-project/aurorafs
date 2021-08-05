// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/ethersphere/bee/cmd/bee/cmd"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func main() {
	go func(){
		http.ListenAndServe("0.0.0.0:6060", nil)

	}()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
