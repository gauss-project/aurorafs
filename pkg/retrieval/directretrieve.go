package retrieval

import (
	"context"
	"fmt"
	// "os"
	"time"

	// "errors"
	// "fmt"
	// "strconv"
	// "time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	"github.com/gauss-project/aurorafs/pkg/routetab"

	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"

	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	// pb "github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	"github.com/gauss-project/aurorafs/pkg/soc"
	"github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/opentracing/opentracing-go"

	"github.com/gauss-project/aurorafs/pkg/chunkinfo"

	"golang.org/x/sync/singleflight"
)

type ACOService struct {
	addr          		boson.Address
	streamer      		p2p.Streamer
	p2p_service			p2p.Service
	peerSuggester 		topology.EachPeerer
	storer        		storage.Storer
	singleflight  		singleflight.Group
	logger        		logging.Logger
	//accounting    	accounting.Interface
	//pricer        	accounting.Pricer
	metrics 			metrics
	tracer  			*tracing.Tracer

	chunkinfo     		chunkinfo.ChunkInfo
	routetabService  	routetab.Service
}

type retrievalResult struct {
	chunk     		boson.Chunk
	peer      		boson.Address
	download_rate 	float64
	err       		error
	// retrieved bool
}

// const (
// 	protocolName = "aco_retreive"
// 	protocolVersion = "1.0.0"
// )

func (s *ACOService) RetrieveChunk(ctx context.Context, root_addr, chunk_addr boson.Address) (boson.Chunk, error){
	chunk_result := s.chunkinfo.GetChunkInfo(root_addr, chunk_addr)
	node_list := make([]boson.Address, 0)
	for _, v := range chunk_result{
		new_node := boson.NewAddress(v)
		node_list = append(node_list, new_node)
	}

	ticker := time.NewTicker(retrieveRetryIntervalDuration)
	defer ticker.Stop()

	var (
		max_attemp int = 5
		resultC = make(chan retrievalResult, max_attemp)
	)

	aco_index_list := s.getACONodeList(node_list, max_attemp)

	for node_index := range aco_index_list{
		target_node := node_list[node_index]

		go func(){
			chunk, download_rate, err := s.retrieveChunk(ctx, target_node, root_addr, chunk_addr)
			select {
			case resultC <- retrievalResult{
				chunk:     		chunk,
				download_rate: 	download_rate,
				peer:      		target_node,
				err:       		err,
			}:
			case <-ctx.Done():
			}
		}()

		select{
		case <-ticker.C:
			// break
		case res := <-resultC :
			if res.err != nil{
				s.logger.Debugf("retrieval: failed to get chunk (%s,%s) from peer %s: %v", 
					root_addr, chunk_addr, target_node, res.err)
			}else{
				s.rankNodeDownload(target_node, res.download_rate)
				return res.chunk, nil
			}
		case <-ctx.Done():
			s.logger.Tracef("retrieval: failed to get chunk (%s:%s): %v", root_addr, chunk_addr, ctx.Err())
			return nil, fmt.Errorf("retrieval: %w", ctx.Err())
		}
	}

	return nil, storage.ErrNotFound
}

func (s *ACOService) retrieveChunk(ctx context.Context, target_node boson.Address, root_addr, chunk_addr boson.Address) (boson.Chunk, float64, error){
	// if s.isNeighborNode(target_node){
	// }else{
	// 	// 通过AddressBook获取underlay地址
	// 	return nil, -1, fmt.Errorf("can't connect to %v", target_node)
	// }

	// chunk = boson.NewChunk(addr, d.Data)
	// if !cac.Valid(chunk) {
	// 	if !soc.Valid(chunk) {
	// 		s.metrics.InvalidChunkRetrieved.Inc()
	// 		s.metrics.TotalErrors.Inc()
	// 		return nil, peer, boson.ErrInvalidChunk
	// 	}
	// }

	// return chunk, download_rate, err


	stream, err := s.streamer.NewStream(ctx, target_node, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		s.logger.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	start_time := time.Now()
	w, r := protobuf.NewWriterAndReader(stream)
	if err := w.WriteMsgWithContext(ctx, &pb.RequestChunk{
		RootAddr: root_addr.Bytes(),
		ChunkAddr: chunk_addr.Bytes(),
	}); err != nil {
		s.metrics.TotalErrors.Inc()
		return nil, -1, fmt.Errorf("write request: %w peer %s", err, target_node.String())
	}

	end_time := time.Now()
	var d pb.Delivery
	if err := r.ReadMsgWithContext(ctx, &d); err != nil {
		s.metrics.TotalErrors.Inc()
		return nil, -1, fmt.Errorf("read delivery: %w peer %s", err, target_node.String())
	}
	elapsed := end_time.Unix() - start_time.Unix()
	download_rate := int64(d.Size())/elapsed

	chunk := boson.NewChunk(chunk_addr, d.Data)
	if !cac.Valid(chunk) {
		if !soc.Valid(chunk) {
			s.metrics.InvalidChunkRetrieved.Inc()
			s.metrics.TotalErrors.Inc()
			return nil, -1, boson.ErrInvalidChunk
		}
	}

	// credit the peer after successful delivery
	//err = s.accounting.Credit(peer, chunkPrice)
	//if err != nil {
	//	return nil, peer, err
	//}
	//s.metrics.ChunkPrice.Observe(float64(chunkPrice))

	return chunk, float64(download_rate), err

}

func (s *ACOService) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	var req pb.RequestChunk
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request: %w peer %s", err, p.Address.String())
	}
	span, _, ctx := s.tracer.StartSpanFromContext(ctx, "handle-retrieve-chunk", 
		s.logger, opentracing.Tag{Key: "address", Value: fmt.Sprintf("%v,%v", boson.NewAddress(req.RootAddr).String(), boson.NewAddress(req.ChunkAddr).String()))},
	)
	defer span.Finish()

	ctx = context.WithValue(ctx, requestSourceContextKey{}, p.Address.String())
	_, chunk_addr := boson.NewAddress(req.RootAddr), boson.NewAddress(req.ChunkAddr)

	chunk, err := s.storer.Get(ctx, storage.ModeGetRequest, chunk_addr)
	if err != nil {
		// if errors.Is(err, storage.ErrNotFound) {
		// 	// forward the request
		// 	chunk, err = s.RetrieveChunk(ctx, addr)
		// 	if err != nil {
		// 		return fmt.Errorf("retrieve chunk: %w", err)
		// 	}
		// } else {
		// 	return fmt.Errorf("get from store: %w", err)
		// }
		return fmt.Errorf("get from store: %w", err)
	}

	if err := w.WriteMsgWithContext(ctx, &pb.Delivery{
		Data: chunk.Data(),
	}); err != nil {
		return fmt.Errorf("write delivery: %w peer %s", err, p.Address.String())
	}

	s.logger.Tracef("retrieval protocol debiting peer %s", p.Address.String())

	// compute the price we charge for this chunk and debit it from p's balance
	//chunkPrice := s.pricer.Price(chunk.Address())
	//err = s.accounting.Debit(p.Address, chunkPrice)
	//if err != nil {
	//	return err
	//}

	return nil
}

func (s *ACOService) getACONodeList(optional_node_list []boson.Address, node_count int)([]int){
	index_count := node_count
	if node_count > len(optional_node_list){
		index_count = len(optional_node_list)
	}

	index_list := make([]int, index_count)

	for k, _ := range index_list{
		index_list[k] = k
	}
	return index_list
}

func (s *ACOService) rankNodeDownload(node boson.Address, download_rate float64){
	// do nothing
	return
}

func (s *ACOService) isNeighborNode(node_addr boson.Address)bool{
	var result bool

	_ = s.peerSuggester.EachPeer(func(peer boson.Address, po uint8) (bool, bool, error) {
		if peer.Equal(node_addr){
			result = true
			return true, false, nil		// stop review
		}
		return false, true, nil			// jump to next
	})

	return result
}