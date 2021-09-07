package retrieval

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/retrieval/aco"
	"github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	"github.com/gauss-project/aurorafs/pkg/routetab"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"

	"github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/soc"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/opentracing/opentracing-go"

	"github.com/gauss-project/aurorafs/pkg/chunkinfo"

	// "golang.org/x/sync/singleflight"
)

type requestSourceContextKey struct{}

const (
	protocolName    = "retrieval"
	protocolVersion = "1.0.0"
	streamName      = "retrieval"
)

var _ Interface = (*Service)(nil)

type Interface interface {
	RetrieveChunk(ctx context.Context, rootAddr, chunkAddr boson.Address) (chunk boson.Chunk, err error)
}

type Service struct {
	addr          		boson.Address
	streamer      		p2p.Streamer
	storer        		storage.Storer
	logger        		logging.Logger
	metrics 			metrics
	tracer  			*tracing.Tracer
	chunkinfo     		chunkinfo.Interface
	acoServer			*aco.AcoServer
	routeTab        	routetab.RouteTab
}

type retrievalResult struct {
	chunk     		boson.Chunk
	downloadDetail	*aco.DownloadDetail
	err       		error
}

const (
	maxPeers             = 5
	retrieveChunkTimeout = 10 * time.Second
	retrieveRetryIntervalDuration = 5 * time.Second
)

func New(addr boson.Address, streamer p2p.Streamer, routeTable routetab.RouteTab, storer storage.Storer, logger logging.Logger, tracer *tracing.Tracer) *Service {
	acoServer := aco.NewAcoServer()
	return &Service{
		addr:          addr,
		streamer:      streamer,
		storer:        storer,
		logger:        logger,
		metrics: newMetrics(),
		tracer:  tracer,
		acoServer: &acoServer,
		routeTab: routeTable,
	}
}

func (s *Service) Config(chunkInfo chunkinfo.Interface){
	s.chunkinfo = chunkInfo
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
	}
}

func (s *Service) RetrieveChunk(ctx context.Context, rootAddr, chunkAddr boson.Address) (chunk boson.Chunk, err error){
	s.metrics.RequestCounter.Inc()
	ticker := time.NewTicker(retrieveRetryIntervalDuration)
	defer ticker.Stop()

	var (
		totalRouteCount int = 5
		resultC = make(chan retrievalResult, totalRouteCount)
		// resultC = make(chan retrievalResult, totalRouteCount)
	)

	chunkResult := s.chunkinfo.GetChunkInfo(rootAddr, chunkAddr)
	if len(chunkResult) == 0{
		return nil, fmt.Errorf("no result from chunkinfo")
	}

	routeList := make([]aco.Route, 0)
	// routeCount := 0
	for _, v := range chunkResult {
		addr := boson.NewAddress(v)
		newRoute := aco.NewRoute(addr, addr)
		routeList = append(routeList, newRoute)
		// routeCount += 1
		// if routeCount >= totalRouteCount{
		// 	break
		// } 
	}

	acoRouteIndexList := s.acoServer.GetRouteAcoIndex(routeList, totalRouteCount)

	for _, routeIndex := range acoRouteIndexList{
		retrievalRoute := routeList[routeIndex]

		s.metrics.PeerRequestCounter.Inc()

		go func(){
			s.acoServer.OnDownloadStart(retrievalRoute)
			chunk, downloadDetail, err := s.retrieveChunk(ctx, retrievalRoute, rootAddr, chunkAddr)
			select {
			case resultC <- retrievalResult{
				chunk:     			chunk,
				downloadDetail: 	downloadDetail,
				err:       			err,
			}:
			case <-ctx.Done():
			}
		}()

		select{
		case <-ticker.C:
			// continue next route
		case result := <-resultC :
			if result.err != nil{
				s.logger.Debugf("retrieval: failed to get chunk (%s,%s) from route %s: %v", 
					rootAddr, chunkAddr, retrievalRoute, result.err)
				s.acoServer.OnDownloadFinish(retrievalRoute, nil)
				// return nil, result.err
			}else{
				s.acoServer.OnDownloadFinish(retrievalRoute, result.downloadDetail)
				s.chunkinfo.OnChunkTransferred(chunkAddr, rootAddr, s.addr)
				return result.chunk, nil
			}
		case <-ctx.Done():
			s.acoServer.OnDownloadFinish(retrievalRoute, nil)
			s.logger.Tracef("retrieval: failed to get chunk: ctx.Done() (%s:%s): %v", rootAddr, chunkAddr, ctx.Err())
			return nil, fmt.Errorf("retrieval: %w", ctx.Err())
		}
	}

	return nil, storage.ErrNotFound
}

func (s *Service) retrieveChunk(ctx context.Context, route aco.Route, rootAddr, chunkAddr boson.Address) (boson.Chunk, *aco.DownloadDetail, error){
	if	!route.LinkNode.Equal(route.TargetNode){
		return nil, nil, fmt.Errorf("not direct link, %v,%v(not same)", route.LinkNode.String(), route.TargetNode.String())
	}
	if err := s.routeTab.Connect(ctx, route.LinkNode); err != nil{
		return nil, nil, fmt.Errorf("connect failed");
	}

	stream, err := s.streamer.NewStream(ctx, route.LinkNode, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		s.metrics.TotalErrors.Inc()
		s.logger.Errorf("new stream: %w", err)
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	startMs := time.Now().UnixNano()/1e6
	w, r := protobuf.NewWriterAndReader(stream)
	if err := w.WriteMsgWithContext(ctx, &pb.RequestChunk{
		TargetAddr: route.TargetNode.Bytes(),
		RootAddr: rootAddr.Bytes(),
		ChunkAddr: chunkAddr.Bytes(),
	}); err != nil {
		s.metrics.TotalErrors.Inc()
		return nil, nil, fmt.Errorf("write request: %w route %v,%v", err, route.LinkNode.String(), route.TargetNode.String())
	}

	var d pb.Delivery
	if err := r.ReadMsgWithContext(ctx, &d); err != nil {
		s.metrics.TotalErrors.Inc()
		return nil, nil, fmt.Errorf("read delivery: %w route %v,%v", err, route.LinkNode.String(), route.TargetNode.String())
	}
	endMs := time.Now().UnixNano()/1e6
	dataSize := d.Size()

	downloadDetail := aco.DownloadDetail{
		StartMs: startMs,
		EndMs: endMs,
		Size: int64(dataSize),
	}

	chunk := boson.NewChunk(chunkAddr, d.Data)
	if !cac.Valid(chunk) {
		if !soc.Valid(chunk) {
			s.metrics.InvalidChunkRetrieved.Inc()
			s.metrics.TotalErrors.Inc()
			return nil, nil, boson.ErrInvalidChunk
		}
	}

	// exists, err := s.storer.Put(ctx, storage.ModePutRequest, chunk)
	// if err != nil {
	// 	s.metrics.TotalErrors.Inc()
	// 	return nil, nil, err
	// }
	// if !exists[0] {
	// 	s.chunkinfo.OnChunkTransferred(chunkAddr, rootAddr, s.addr)
	// }
	// s.chunkinfo.OnChunkTransferred(chunkAddr, rootAddr, s.addr)

	return chunk, &downloadDetail, nil 
}


func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	fmt.Printf("handler addr: %v\n", s.addr.String())

	var req pb.RequestChunk
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request: %w peer %s", err, p.Address.String())
	}
	span, _, ctx := s.tracer.StartSpanFromContext(ctx, "handle-retrieve-chunk", 
		s.logger, opentracing.Tag{Key: "address", Value: fmt.Sprintf("%v,%v", boson.NewAddress(req.RootAddr).String(), boson.NewAddress(req.ChunkAddr).String())},
	)
	defer span.Finish()

	ctx = context.WithValue(ctx, requestSourceContextKey{}, p.Address.String())
	_, rootAddr, chunkAddr := boson.NewAddress(req.TargetAddr), boson.NewAddress(req.RootAddr), boson.NewAddress(req.ChunkAddr)

	forward := false
	chunk, err := s.storer.Get(ctx, storage.ModeGetRequest, chunkAddr)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// forward the request
			// if !s.addr.Equal(targetAddr){
				chunk, err = s.RetrieveChunk(ctx, rootAddr, chunkAddr)
				if err != nil {
					return fmt.Errorf("retrieve chunk: %w", err)
				}
				forward = true
			// }else{
			// 	return fmt.Errorf("get from store: %w", err)
			// }
		} else {
			return fmt.Errorf("get from store: %w", err)
		}
	}

	if err := w.WriteMsgWithContext(ctx, &pb.Delivery{
		Data: chunk.Data(),
	}); err != nil {
		return fmt.Errorf("write delivery: %w peer %s", err, p.Address.String())
	}

	if s.chunkinfo != nil{
		s.chunkinfo.OnChunkTransferred(chunkAddr, rootAddr, p.Address)
	}

	if forward{
		_, err = s.storer.Put(ctx, storage.ModePutRequest, chunk)
		if err != nil{
			return fmt.Errorf("retrieve cache put :%w", err)
		}
	}

	s.logger.Tracef("retrieval protocol debiting peer %s", p.Address.String())

	return nil
}
