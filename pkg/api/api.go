// Package api provides the functionality of the aurora
// client-facing HTTP API.
package api

import (
	"context"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"io"
	"math"
	"net/http"

	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/logging"
	m "github.com/gauss-project/aurorafs/pkg/metrics"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/resolver"
	"github.com/gauss-project/aurorafs/pkg/storage"

	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

const (
	BosonPinHeader           = "Boson-Pin"
	BosonTagHeader           = "Boson-Tag"
	BosonEncryptHeader       = "Boson-Encrypt"
	BosonIndexDocumentHeader = "Boson-Index-Document"
	BosonErrorDocumentHeader = "Boson-Error-Document"
	BosonFeedIndexHeader     = "Boson-Feed-Index"
	BosonFeedIndexNextHeader = "Boson-Feed-Index-Next"
)

// The size of buffer used for prefetching content with Langos.
// Warning: This value influences the number of chunk requests and chunker join goroutines
// per file request.
// Recommended value is 8 or 16 times the io.Copy default buffer value which is 32kB, depending
// on the file size. Use lookaheadBufferSize() to get the correct buffer size for the request.
const (
	smallFileBufferSize = 8 * 32 * 1024 * 8
	largeFileBufferSize = 16 * 32 * 1024 * 8

	largeBufferFilesizeThreshold = 10 * 1000000 // ten megs
)

var (
	errInvalidNameOrAddress = errors.New("invalid name or aurora address")
	errNoResolver           = errors.New("no resolver connected")
)

// Service is the API service interface.
type Service interface {
	http.Handler
	m.Collector
	io.Closer
}

type server struct {
	storer   storage.Storer
	resolver resolver.Interface

	overlay   boson.Address
	chunkInfo chunkinfo.Interface
	traversal traversal.Service
	logger    logging.Logger
	tracer    *tracing.Tracer

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
}

const (
	// TargetsRecoveryHeader defines the Header for Recovery targets in Global Pinning
	TargetsRecoveryHeader = "boson-recovery-targets"
)

// New will create a and initialize a new API service.
func New(storer storage.Storer, resolver resolver.Interface, addr boson.Address, chunkInfo chunkinfo.Interface, traversalService traversal.Service, logger logging.Logger, tracer *tracing.Tracer, o Options) Service {
	s := &server{

		storer:   storer,
		resolver: resolver,

		overlay:   addr,
		chunkInfo: chunkInfo,
		traversal: traversalService,

		Options: o,
		logger:  logger,
		tracer:  tracer,
		metrics: newMetrics(),
		quit:    make(chan struct{}),
	}

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
	return boson.ZeroAddress, err

	//// If no resolver is not available, return an error.
	//if s.resolver == nil {
	//	return boson.ZeroAddress, errNoResolver
	//}
	//
	//// Try and resolve the name using the provided resolver.
	//log.Debugf("name resolve: attempting to resolve %s to aurora address", str)
	//addr, err = s.resolver.Resolve(str)
	//if err == nil {
	//	log.Tracef("name resolve: resolved name %s to %s", str, addr)
	//	return addr, nil
	//}
	//
	//return boson.ZeroAddress, fmt.Errorf("%w: %v", errInvalidNameOrAddress, err)
}

// requestModePut returns the desired storage.ModePut for this request based on the request headers.
func requestModePut(r *http.Request) storage.ModePut {
	if h := strings.ToLower(r.Header.Get(BosonPinHeader)); h == "true" {
		return storage.ModePutUploadPin
	}

	return storage.ModePutUpload
}

func requestEncrypt(r *http.Request) bool {
	return strings.ToLower(r.Header.Get(BosonEncryptHeader)) == "true"
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
	if size <= largeBufferFilesizeThreshold {
		return smallFileBufferSize
	}
	return largeFileBufferSize
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

type pipelineFunc func(context.Context, io.Reader, int64) (boson.Address, error)

func requestPipelineFn(s storage.Storer, r *http.Request) pipelineFunc {
	mode, encrypt := requestModePut(r), requestEncrypt(r)
	return func(ctx context.Context, r io.Reader, l int64) (boson.Address, error) {
		pipe := builder.NewPipelineBuilder(ctx, s, mode, encrypt)
		return builder.FeedPipeline(ctx, pipe, r, l)
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
