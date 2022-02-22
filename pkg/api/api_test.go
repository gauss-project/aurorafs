package api_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/api"
	mockauth "github.com/gauss-project/aurorafs/pkg/auth/mock"
	"github.com/gauss-project/aurorafs/pkg/boson"
	chunkinfo "github.com/gauss-project/aurorafs/pkg/chunkinfo/mock"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/pinning"
	"github.com/gauss-project/aurorafs/pkg/resolver"
	resolverMock "github.com/gauss-project/aurorafs/pkg/resolver/mock"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"github.com/gorilla/websocket"
	"resenje.org/web"
)

type testServerOptions struct {
	Storer             storage.Storer
	Resolver           resolver.Interface
	Traversal          traversal.Traverser
	Pinning            pinning.Interface
	WsPath             string
	GatewayMode        bool
	WsPingPeriod       time.Duration
	Logger             logging.Logger
	PreventRedirect    bool
	CORSAllowedOrigins []string
	ChunkInfo          *chunkinfo.ChunkInfo
	Authenticator      *mockauth.Auth
	Restricted         bool
}

func newTestServer(t *testing.T, o testServerOptions) (*http.Client, *websocket.Conn, string) {
	if o.Logger == nil {
		o.Logger = logging.New(io.Discard, 0)
	}
	if o.Resolver == nil {
		o.Resolver = resolverMock.NewResolver()
	}
	if o.WsPingPeriod == 0 {
		o.WsPingPeriod = 60 * time.Second
	}
	if o.Authenticator == nil {
		o.Authenticator = &mockauth.Auth{}
	}
	serverAddress := boson.MustParseHexAddress("01")

	s := api.New(o.Storer, o.Resolver, serverAddress, o.ChunkInfo, o.Traversal, o.Pinning, o.Authenticator, o.Logger, nil, nil, nil, nil, nil, nil,
		api.Options{
			CORSAllowedOrigins: o.CORSAllowedOrigins,
			GatewayMode:        o.GatewayMode,
			WsPingPeriod:       60 * time.Second,
			Restricted:         o.Restricted,
		})

	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	var (
		httpClient = &http.Client{
			Transport: web.RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
				u, err := url.Parse(ts.URL + r.URL.String()) // ts.URL
				if err != nil {
					return nil, err
				}
				r.URL = u
				return ts.Client().Transport.RoundTrip(r)
			}),
		}
		conn *websocket.Conn
		err  error
	)

	if o.WsPath != "" {
		u := url.URL{Scheme: "ws", Host: ts.Listener.Addr().String(), Path: o.WsPath}
		conn, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			t.Fatalf("dial: %v. url %v", err, u.String())
		}
	}

	if o.PreventRedirect {
		httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	return httpClient, conn, ts.Listener.Addr().String()
}

func request(t *testing.T, client *http.Client, method, resource string, body io.Reader, responseCode int) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, resource, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != responseCode {
		t.Fatalf("got response status %s, want %v %s", resp.Status, responseCode, http.StatusText(responseCode))
	}
	return resp
}

func TestParseName(t *testing.T) {
	const auroraHash = "89c17d0d8018a19057314aa035e61c9d23c47581a61dd3a79a7839692c617e4d"

	testCases := []struct {
		desc       string
		name       string
		log        logging.Logger
		res        resolver.Interface
		noResolver bool
		wantAdr    boson.Address
		wantErr    error
	}{
		{
			desc:    "empty name",
			name:    "",
			wantErr: api.ErrInvalidNameOrAddress,
		},
		{
			desc:    "aurora hash",
			name:    auroraHash,
			wantAdr: boson.MustParseHexAddress(auroraHash),
		},
		{
			desc:       "no resolver connected with aurora hash",
			name:       auroraHash,
			noResolver: true,
			wantAdr:    boson.MustParseHexAddress(auroraHash),
		},
		{
			desc:       "no resolver connected with name",
			name:       "itdoesntmatter.eth",
			noResolver: true,
			wantErr:    api.ErrNoResolver,
		},
		{
			desc: "name not resolved",
			name: "not.good",
			res: resolverMock.NewResolver(
				resolverMock.WithResolveFunc(func(string) (boson.Address, error) {
					return boson.ZeroAddress, errors.New("failed to resolve")
				}),
			),
			wantErr: api.ErrInvalidNameOrAddress,
		},
		{
			desc:    "name resolved",
			name:    "everything.okay",
			wantAdr: boson.MustParseHexAddress("89c17d0d8018a19057314aa035e61c9d23c47581a61dd3a79a7839692c617e4d"),
		},
	}
	for _, tC := range testCases {
		if tC.log == nil {
			tC.log = logging.New(io.Discard, 0)
		}
		if tC.res == nil && !tC.noResolver {
			tC.res = resolverMock.NewResolver(
				resolverMock.WithResolveFunc(func(string) (boson.Address, error) {
					return tC.wantAdr, nil
				}))
		}

		// s := api.New(nil, tC.res, nil, nil, tC.log, nil, api.Options{}).(*api.Server)

		// t.Run(tC.desc, func(t *testing.T) {
		//	got, err := s.ResolveNameOrAddress(tC.name)
		//	if err != nil && !errors.Is(err, tC.wantErr) {
		//		t.Fatalf("bad error: %v", err)
		//	}
		//	if !got.Equal(tC.wantAdr) {
		//		t.Errorf("got %s, want %s", got, tC.wantAdr)
		//	}
		//
		// })
	}
}

// TestCalculateNumberOfChunks is a unit test for
// the chunk-number-according-to-content-length calculation.
func TestCalculateNumberOfChunks(t *testing.T) {
	for _, tc := range []struct{ len, chunks int64 }{
		{len: 1000, chunks: 1},
		{len: 5000, chunks: 3},
		{len: 10000, chunks: 4},
		{len: 100000, chunks: 26},
		{len: 1000000, chunks: 248},
		{len: 325839339210, chunks: 79550620 + 621490 + 4856 + 38 + 1},
	} {
		res := api.CalculateNumberOfChunks(tc.len, false)
		if res != tc.chunks {
			t.Fatalf("expected result for %d bytes to be %d got %d", tc.len, tc.chunks, res)
		}
	}
}

// TestCalculateNumberOfChunksEncrypted is a unit test for
// the chunk-number-according-to-content-length calculation with encryption
// (branching factor=64)
func TestCalculateNumberOfChunksEncrypted(t *testing.T) {
	for _, tc := range []struct{ len, chunks int64 }{
		{len: 1000, chunks: 1},
		{len: 5000, chunks: 3},
		{len: 10000, chunks: 4},
		{len: 100000, chunks: 26},
		{len: 1000000, chunks: 245 + 4 + 1},
		{len: 325839339210, chunks: 79550620 + 1242979 + 19422 + 304 + 5 + 1},
	} {
		res := api.CalculateNumberOfChunks(tc.len, true)
		if res != tc.chunks {
			t.Fatalf("expected result for %d bytes to be %d got %d", tc.len, tc.chunks, res)
		}
	}
}
