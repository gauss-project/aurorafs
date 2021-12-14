// Package api provides the functionality of the aurora
// client-facing HTTP API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gauss-project/aurorafs/pkg/auth"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	m "github.com/gauss-project/aurorafs/pkg/metrics"
	"github.com/gauss-project/aurorafs/pkg/pinning"
	"github.com/gauss-project/aurorafs/pkg/resolver"
	"github.com/gauss-project/aurorafs/pkg/settlement/chain"
	"github.com/gauss-project/aurorafs/pkg/settlement/traffic"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

const (
	AuroraPinHeader            = "Aurora-Pin"
	AuroraTagHeader            = "Aurora-Tag"
	AuroraEncryptHeader        = "Aurora-Encrypt"
	AuroraIndexDocumentHeader  = "Aurora-Index-Document"
	AuroraErrorDocumentHeader  = "Aurora-Error-Document"
	AuroraFeedIndexHeader      = "Aurora-Feed-Index"
	AuroraFeedIndexNextHeader  = "Aurora-Feed-Index-Next"
	AuroraCollectionHeader     = "Aurora-Collection"
	AuroraCollectionNameHeader = "Aurora-Collection-Name"
)

// The size of buffer used for prefetching content with Langos.
// Warning: This value influences the number of chunk requests and chunker join goroutines
// per file request.
// Recommended value is 8 or 16 times the io.Copy default buffer value which is 32kB, depending
// on the file size. Use lookaheadBufferSize() to get the correct buffer size for the request.
const (
	smallFileBufferSize = 8 * 32 * 1024
	largeFileBufferSize = 16 * 32 * 1024

	largeBufferFilesizeThreshold = 10 * 1000000 // ten megs
)

var BufferSizeMul int

const (
	contentTypeHeader = "Content-Type"
	multiPartFormData = "multipart/form-data"
	contentTypeTar    = "application/x-tar"
)

var (
	errInvalidNameOrAddress = errors.New("invalid name or aurora address")
	errNoResolver           = errors.New("no resolver connected")
	invalidRequest          = errors.New("could not validate request")
	invalidContentType      = errors.New("invalid content-type")
	directoryStoreError     = errors.New("could not store directory")
	fileStoreError          = errors.New("could not store file")
)

// Service is the API service interface.
type Service interface {
	http.Handler
	m.Collector
	io.Closer
}

type authenticator interface {
	Authorize(string) bool
	GenerateKey(string, int) (string, error)
	RefreshKey(string, int) (string, error)
	Enforce(string, string, string) (bool, error)
}

type server struct {
	auth        authenticator
	storer      storage.Storer
	resolver    resolver.Interface
	overlay     boson.Address
	chunkInfo   chunkinfo.Interface
	traversal   traversal.Traverser
	pinning     pinning.Interface
	logger      logging.Logger
	tracer      *tracing.Tracer
	traffic     traffic.ApiInterface
	commonChain chain.Common
	Options
	http.Handler
	metrics metrics

	wsWg sync.WaitGroup // wait for all websockets to close on exit
	quit chan struct{}
}

type Options struct {
	CORSAllowedOrigins []string
	GatewayMode        bool
	WsPingPeriod       time.Duration
	BufferSizeMul      int
	Restricted         bool
}

const (
	// TargetsRecoveryHeader defines the Header for Recovery targets in Global Pinning
	TargetsRecoveryHeader = "aurora-recovery-targets"
)

// New will create a and initialize a new API service.
func New(storer storage.Storer, resolver resolver.Interface, addr boson.Address, chunkInfo chunkinfo.Interface,
	traversalService traversal.Traverser, pinning pinning.Interface, auth authenticator, logger logging.Logger,
	tracer *tracing.Tracer, traffic traffic.ApiInterface, commonChain chain.Common, o Options) Service {
	s := &server{
		auth:        auth,
		storer:      storer,
		resolver:    resolver,
		overlay:     addr,
		chunkInfo:   chunkInfo,
		traversal:   traversalService,
		pinning:     pinning,
		Options:     o,
		logger:      logger,
		tracer:      tracer,
		commonChain: commonChain,
		metrics:     newMetrics(),
		quit:        make(chan struct{}),
		traffic:     traffic,
	}

	BufferSizeMul = o.BufferSizeMul
	s.setupRouting()

	return s
}

// Close hangs up running websockets on shutdown.
func (s *server) Close() error {
	s.logger.Info("api shutting down")
	close(s.quit)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wsWg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return errors.New("api shutting down with open websockets")
	}

	return nil
}

func (s *server) resolveNameOrAddress(str string) (boson.Address, error) {
	log := s.logger

	// Try and parse the name as a boson address.
	addr, err := boson.ParseHexAddress(str)
	if err == nil {
		log.Tracef("name resolve: valid aurora address %q", str)
		return addr, nil
	}

	// If no resolver is not available, return an error.
	if s.resolver == nil {
		return boson.ZeroAddress, errNoResolver
	}

	// Try and resolve the name using the provided resolver.
	log.Debugf("name resolve: attempting to resolve %s to aurora address", str)
	addr, err = s.resolver.Resolve(str)
	if err == nil {
		log.Tracef("name resolve: resolved name %s to %s", str, addr)
		return addr, nil
	}

	return boson.ZeroAddress, fmt.Errorf("%w: %v", errInvalidNameOrAddress, err)
}

// requestModePut returns the desired storage.ModePut for this request based on the request headers.
func requestModePut(r *http.Request) storage.ModePut {
	if h := strings.ToLower(r.Header.Get(AuroraPinHeader)); h == "true" {
		return storage.ModePutUploadPin
	}

	return storage.ModePutUpload
}

func requestEncrypt(r *http.Request) bool {
	return strings.ToLower(r.Header.Get(AuroraEncryptHeader)) == "true"
}

type securityTokenRsp struct {
	Key string `json:"key"`
}

type securityTokenReq struct {
	Role   string `json:"role"`
	Expiry int    `json:"expiry"`
}

func (s *server) authHandler(w http.ResponseWriter, r *http.Request) {
	_, pass, ok := r.BasicAuth()

	if !ok {
		s.logger.Error("api: auth handler: missing basic auth")
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		jsonhttp.Unauthorized(w, "Unauthorized")
		return
	}

	if !s.auth.Authorize(pass) {
		s.logger.Error("api: auth handler: unauthorized")
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		jsonhttp.Unauthorized(w, "Unauthorized")
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.logger.Debugf("api: auth handler: read request body: %v", err)
		s.logger.Error("api: auth handler: read request body")
		jsonhttp.BadRequest(w, "Read request body")
		return
	}

	var payload securityTokenReq
	if err = json.Unmarshal(body, &payload); err != nil {
		s.logger.Debugf("api: auth handler: unmarshal request body: %v", err)
		s.logger.Error("api: auth handler: unmarshal request body")
		jsonhttp.BadRequest(w, "Unmarshal json body")
		return
	}

	key, err := s.auth.GenerateKey(payload.Role, payload.Expiry)
	if errors.Is(err, auth.ErrExpiry) {
		s.logger.Debugf("api: auth handler: generate key: %v", err)
		s.logger.Error("api: auth handler: generate key")
		jsonhttp.BadRequest(w, "Expiry duration must be a positive number")
		return
	}
	if err != nil {
		s.logger.Debugf("api: auth handler: add auth token: %v", err)
		s.logger.Error("api: auth handler: add auth token")
		jsonhttp.InternalServerError(w, "Error generating authorization token")
		return
	}

	jsonhttp.Created(w, securityTokenRsp{
		Key: key,
	})
}

func (s *server) refreshHandler(w http.ResponseWriter, r *http.Request) {
	reqToken := r.Header.Get("Authorization")
	if !strings.HasPrefix(reqToken, "Bearer ") {
		jsonhttp.Forbidden(w, "Missing bearer token")
		return
	}

	keys := strings.Split(reqToken, "Bearer ")

	if len(keys) != 2 || strings.Trim(keys[1], " ") == "" {
		jsonhttp.Forbidden(w, "Missing security token")
		return
	}

	authToken := keys[1]

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.logger.Debugf("api: auth handler: read request body: %v", err)
		s.logger.Error("api: auth handler: read request body")
		jsonhttp.BadRequest(w, "Read request body")
		return
	}

	var payload securityTokenReq
	if err = json.Unmarshal(body, &payload); err != nil {
		s.logger.Debugf("api: auth handler: unmarshal request body: %v", err)
		s.logger.Error("api: auth handler: unmarshal request body")
		jsonhttp.BadRequest(w, "Unmarshal json body")
		return
	}

	key, err := s.auth.RefreshKey(authToken, payload.Expiry)
	if errors.Is(err, auth.ErrTokenExpired) {
		s.logger.Debugf("api: auth handler: refresh key: %v", err)
		s.logger.Error("api: auth handler: refresh key")
		jsonhttp.BadRequest(w, "Token expired")
		return
	}

	if err != nil {
		s.logger.Debugf("api: auth handler: refresh token: %v", err)
		s.logger.Error("api: auth handler: refresh token")
		jsonhttp.InternalServerError(w, "Error refreshing authorization token")
		return
	}

	jsonhttp.Created(w, securityTokenRsp{
		Key: key,
	})
}

func (s *server) newTracingHandler(spanName string) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, err := s.tracer.WithContextFromHTTPHeaders(r.Context(), r.Header)
			if err != nil && !errors.Is(err, tracing.ErrContextNotFound) {
				s.logger.Debugf("span '%s': extract tracing context: %v", spanName, err)
				// ignore
			}

			span, _, ctx := s.tracer.StartSpanFromContext(ctx, spanName, s.logger)
			defer span.Finish()

			err = s.tracer.AddContextHTTPHeader(ctx, r.Header)
			if err != nil {
				s.logger.Debugf("span '%s': inject tracing context: %v", spanName, err)
				// ignore
			}

			h.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func lookaheadBufferSize(size int64) int {
	if BufferSizeMul < 1 {
		BufferSizeMul = 8 // default 2mb/4mb
	}
	if size <= largeBufferFilesizeThreshold {
		return smallFileBufferSize * BufferSizeMul
	}
	return largeFileBufferSize * BufferSizeMul
}

// checkOrigin returns true if the origin is not set or is equal to the request host.
func (s *server) checkOrigin(r *http.Request) bool {
	origin := r.Header["Origin"]
	if len(origin) == 0 {
		return true
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	hosts := append(s.CORSAllowedOrigins, scheme+"://"+r.Host)
	for _, v := range hosts {
		if equalASCIIFold(origin[0], v) || v == "*" {
			return true
		}
	}

	return false
}

// equalASCIIFold returns true if s is equal to t with ASCII case folding as
// defined in RFC 4790.
func equalASCIIFold(s, t string) bool {
	for s != "" && t != "" {
		sr, size := utf8.DecodeRuneInString(s)
		s = s[size:]
		tr, size := utf8.DecodeRuneInString(t)
		t = t[size:]
		if sr == tr {
			continue
		}
		if 'A' <= sr && sr <= 'Z' {
			sr = sr + 'a' - 'A'
		}
		if 'A' <= tr && tr <= 'Z' {
			tr = tr + 'a' - 'A'
		}
		if sr != tr {
			return false
		}
	}
	return s == t
}

type pipelineFunc func(context.Context, io.Reader) (boson.Address, error)

func requestPipelineFn(s storage.Storer, r *http.Request) pipelineFunc {
	mode, encrypt := requestModePut(r), requestEncrypt(r)
	return func(ctx context.Context, r io.Reader) (boson.Address, error) {
		pipe := builder.NewPipelineBuilder(ctx, s, mode, encrypt)
		return builder.FeedPipeline(ctx, pipe, r)
	}
}

func requestPipelineFactory(ctx context.Context, s storage.Putter, r *http.Request) func() pipeline.Interface {
	mode, encrypt := requestModePut(r), requestEncrypt(r)
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(ctx, s, mode, encrypt)
	}
}

// calculateNumberOfChunks calculates the number of chunks in an arbitrary
// content length.
func calculateNumberOfChunks(contentLength int64, isEncrypted bool) int64 {
	if contentLength <= boson.ChunkSize {
		return 1
	}
	branchingFactor := boson.Branches
	if isEncrypted {
		branchingFactor = boson.EncryptedBranches
	}

	dataChunks := math.Ceil(float64(contentLength) / float64(boson.ChunkSize))
	totalChunks := dataChunks
	intermediate := dataChunks / float64(branchingFactor)

	for intermediate > 1 {
		totalChunks += math.Ceil(intermediate)
		intermediate = intermediate / float64(branchingFactor)
	}

	return int64(totalChunks) + 1
}

//func requestCalculateNumberOfChunks(r *http.Request) int64 {
//	if !strings.Contains(r.Header.Get(contentTypeHeader), "multipart") && r.ContentLength > 0 {
//		return calculateNumberOfChunks(r.ContentLength, requestEncrypt(r))
//	}
//	return 0
//}
