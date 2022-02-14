package node

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var logs = logging.New(os.Stdout, logrus.TraceLevel)

func testNodeConfig() Config {
	return Config{}
}

// Tests that an empty protocol stack can be closed more than once.
func TestNodeCloseMultipleTimes(t *testing.T) {
	stack, err := NewRPC(logs, testNodeConfig())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}
	stack.Close()

	// Ensure that a stopped node can be stopped again
	for i := 0; i < 3; i++ {
		if err := stack.Close(); err != ErrNodeStopped {
			t.Fatalf("iter %d: stop failure mismatch: have %v, want %v", i, err, ErrNodeStopped)
		}
	}
}

func TestNodeStartMultipleTimes(t *testing.T) {
	stack, err := NewRPC(logs, testNodeConfig())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}

	// Ensure that a node can be successfully started, but only once
	if err := stack.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	if err := stack.Start(); err != ErrNodeRunning {
		t.Fatalf("start failure mismatch: have %v, want %v ", err, ErrNodeRunning)
	}
	// Ensure that a node can be stopped, but only once
	if err := stack.Close(); err != nil {
		t.Fatalf("failed to stop node: %v", err)
	}
	if err := stack.Close(); err != ErrNodeStopped {
		t.Fatalf("stop failure mismatch: have %v, want %v ", err, ErrNodeStopped)
	}
}

// Tests whether a handler can be successfully mounted on the canonical HTTP server
// on the given prefix
func TestRegisterHandler_Successful(t *testing.T) {
	node := createNode(t, 7878, 7979)
	defer node.Close()
	// create and mount handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("success"))
	})
	node.RegisterHandler("test", "/test", handler)

	// start node
	if err := node.Start(); err != nil {
		t.Fatalf("could not start node: %v", err)
	}

	// create HTTP request
	httpReq, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:7878/test", nil)
	if err != nil {
		t.Error("could not issue new http request ", err)
	}

	// check response
	resp := doHTTPRequest(t, httpReq)
	buf := make([]byte, 7)
	_, err = io.ReadFull(resp.Body, buf)
	if err != nil {
		t.Fatalf("could not read response: %v", err)
	}
	assert.Equal(t, "success", string(buf))
}

// Tests that the given handler will not be successfully mounted since no HTTP server
// is enabled for RPC
func TestRegisterHandler_Unsuccessful(t *testing.T) {
	node, err := NewRPC(logs, testNodeConfig())
	if err != nil {
		t.Fatalf("could not create new node: %v", err)
	}

	// create and mount handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("success"))
	})
	node.RegisterHandler("test", "/test", handler)
}

// Tests whether websocket requests can be handled on the same port as a regular http server.
func TestWebsocketHTTPOnSamePort_WebsocketRequest(t *testing.T) {
	node := startHTTP(t, 0, 0)
	defer node.Close()

	ws := strings.Replace(node.HTTPEndpoint(), "http://", "ws://", 1)

	if node.WSEndpoint() != ws {
		t.Fatalf("endpoints should be the same")
	}
	if !checkRPC(ws) {
		t.Fatalf("ws request failed")
	}
	if !checkRPC(node.HTTPEndpoint()) {
		t.Fatalf("http request failed")
	}
}

func TestWebsocketHTTPOnSeparatePort_WSRequest(t *testing.T) {
	// try and get a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("can't listen:", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	node := startHTTP(t, 0, port)
	defer node.Close()

	wsOnHTTP := strings.Replace(node.HTTPEndpoint(), "http://", "ws://", 1)
	ws := fmt.Sprintf("ws://127.0.0.1:%d", port)

	if node.WSEndpoint() == wsOnHTTP {
		t.Fatalf("endpoints should not be the same")
	}
	// ensure ws endpoint matches the expected endpoint
	if node.WSEndpoint() != ws {
		t.Fatalf("ws endpoint is incorrect: expected %s, got %s", ws, node.WSEndpoint())
	}

	if !checkRPC(ws) {
		t.Fatalf("ws request failed")
	}
	if !checkRPC(node.HTTPEndpoint()) {
		t.Fatalf("http request failed")
	}
}

type rpcPrefixTest struct {
	httpPrefix, wsPrefix string
	// These lists paths on which JSON-RPC should be served / not served.
	wantHTTP   []string
	wantNoHTTP []string
	wantWS     []string
	wantNoWS   []string
}

func TestNodeRPCPrefix(t *testing.T) {
	t.Parallel()

	tests := []rpcPrefixTest{
		// both off
		{
			httpPrefix: "", wsPrefix: "",
			wantHTTP:   []string{"/", "/?p=1"},
			wantNoHTTP: []string{"/test", "/test?p=1"},
			wantWS:     []string{"/", "/?p=1"},
			wantNoWS:   []string{"/test", "/test?p=1"},
		},
		// only http prefix
		{
			httpPrefix: "/testprefix", wsPrefix: "",
			wantHTTP:   []string{"/testprefix", "/testprefix?p=1", "/testprefix/x", "/testprefix/x?p=1"},
			wantNoHTTP: []string{"/", "/?p=1", "/test", "/test?p=1"},
			wantWS:     []string{"/", "/?p=1"},
			wantNoWS:   []string{"/testprefix", "/testprefix?p=1", "/test", "/test?p=1"},
		},
		// only ws prefix
		{
			httpPrefix: "", wsPrefix: "/testprefix",
			wantHTTP:   []string{"/", "/?p=1"},
			wantNoHTTP: []string{"/testprefix", "/testprefix?p=1", "/test", "/test?p=1"},
			wantWS:     []string{"/testprefix", "/testprefix?p=1", "/testprefix/x", "/testprefix/x?p=1"},
			wantNoWS:   []string{"/", "/?p=1", "/test", "/test?p=1"},
		},
		// both set
		{
			httpPrefix: "/testprefix", wsPrefix: "/testprefix",
			wantHTTP:   []string{"/testprefix", "/testprefix?p=1", "/testprefix/x", "/testprefix/x?p=1"},
			wantNoHTTP: []string{"/", "/?p=1", "/test", "/test?p=1"},
			wantWS:     []string{"/testprefix", "/testprefix?p=1", "/testprefix/x", "/testprefix/x?p=1"},
			wantNoWS:   []string{"/", "/?p=1", "/test", "/test?p=1"},
		},
	}

	for _, test := range tests {
		test := test
		name := fmt.Sprintf("http=%s ws=%s", test.httpPrefix, test.wsPrefix)
		t.Run(name, func(t *testing.T) {
			cfg := Config{
				HTTPAddr:       "127.0.0.1:0",
				HTTPPathPrefix: test.httpPrefix,
				WSAddr:         "127.0.0.1:0",
				WSPathPrefix:   test.wsPrefix,
			}
			node, err := NewRPC(logs, cfg)
			if err != nil {
				t.Fatal("can't create node:", err)
			}
			defer node.Close()
			if err := node.Start(); err != nil {
				t.Fatal("can't start node:", err)
			}
			test.check(t, node)
		})
	}
}

func (test rpcPrefixTest) check(t *testing.T, node *Node) {
	t.Helper()
	httpBase := "http://" + node.http.listenAddr()
	wsBase := "ws://" + node.http.listenAddr()

	if node.WSEndpoint() != wsBase+test.wsPrefix {
		t.Errorf("Error: node has wrong WSEndpoint %q", node.WSEndpoint())
	}

	for _, path := range test.wantHTTP {
		resp := rpcRequest(t, httpBase+path)
		if resp.StatusCode != 200 {
			t.Errorf("Error: %s: bad status code %d, want 200", path, resp.StatusCode)
		}
	}
	for _, path := range test.wantNoHTTP {
		resp := rpcRequest(t, httpBase+path)
		if resp.StatusCode != 404 {
			t.Errorf("Error: %s: bad status code %d, want 404", path, resp.StatusCode)
		}
	}
	for _, path := range test.wantWS {
		err := wsRequest(t, wsBase+path, "")
		if err != nil {
			t.Errorf("Error: %s: WebSocket connection failed: %v", path, err)
		}
	}
	for _, path := range test.wantNoWS {
		err := wsRequest(t, wsBase+path, "")
		if err == nil {
			t.Errorf("Error: %s: WebSocket connection succeeded for path in wantNoWS", path)
		}

	}
}

func createNode(t *testing.T, httpPort, wsPort int) *Node {
	conf := Config{
		HTTPAddr: fmt.Sprintf("127.0.0.1:%d", httpPort),
		WSAddr:   fmt.Sprintf("127.0.0.1:%d", wsPort),
	}
	node, err := NewRPC(logs, conf)
	if err != nil {
		t.Fatalf("could not create a new node: %v", err)
	}
	return node
}

func startHTTP(t *testing.T, httpPort, wsPort int) *Node {
	node := createNode(t, httpPort, wsPort)
	err := node.Start()
	if err != nil {
		t.Fatalf("could not start http service on node: %v", err)
	}

	return node
}

func doHTTPRequest(t *testing.T, req *http.Request) *http.Response {
	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("could not issue a GET request to the given endpoint: %v", err)

	}
	return resp
}

// checkRPC checks whether JSON-RPC works against the given URL.
func checkRPC(url string) bool {
	c, err := rpc.Dial(url)
	if err != nil {
		return false
	}
	defer c.Close()

	_, err = c.SupportedModules()
	return err == nil
}
