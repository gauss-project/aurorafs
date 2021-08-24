package retrieval_test

import (
	"context"
	"fmt"
	"os"

	// "time"

	// "errors"
	// "fmt"
	// "strconv"
	// "time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/routetab"

	//"github.com/gauss-project/aurorafs/pkg/accounting"
	// "github.com/gauss-project/aurorafs/pkg/cac"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p"

	// "github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	// pb "github.com/gauss-project/aurorafs/pkg/retrieval/pb"
	// "github.com/gauss-project/aurorafs/pkg/soc"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/topology"
	"github.com/gauss-project/aurorafs/pkg/tracing"

	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	// "github.com/gauss-project/aurorafs/pkg/routetab"

	// "github.com/opentracing/opentracing-go"
	"golang.org/x/sync/singleflight"
)

// type Service struct {
// 	addr          boson.Address
// 	streamer      p2p.Streamer
// 	peerSuggester topology.EachPeerer
// 	storer        storage.Storer
// 	singleflight  singleflight.Group
// 	logger        logging.Logger
// 	//accounting    accounting.Interface
// 	//pricer        accounting.Pricer
// 	metrics metrics
// 	tracer  *tracing.Tracer
// }

// func (s *Service) ACORetrieveChunck(ctx context.Context, root_addr, cid_addr boson.Address) (boson.Chunk, error){ flight_key := fmt.Sprintf("%v:%v", root_addr.String, cid_addr.String())
// 	// v, _, err := s.singleflight.Do(ctx, flightRoute, func(ctx context.Context) (interface{}, error) {
// 	// }

// 	v, err, _ := s.singleflight.Do(flight_key, func()(interface{}, error){
// 		ticker := time.NewTicker(retrieveRetryIntervalDuration)
// 		defer ticker.Stop()

// 		var (
// 			peerAttemp int = 0
// 			maxAttemp = 5
// 		)

// 		for{
// 			if peerAttemp < maxAttemp{
// 				peerAttemp++
// 				go func(){
// 					chunk, peer, err := s.acoRetrieveChunk(ctx, root_addr, cid_addr)
// 					if err != nil{
// 						errC <- err
// 						return
// 					}

// 					resultC <- chunk
// 				}()
// 			}else{
// 				ticker.Stop()
// 			}
// 		}

// 	})

// 	return nil, nil
// }

type ACOService struct {
	addr          		boson.Address
	streamer      		p2p.Streamer
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

type TargetNeighborRoute struct {
	targetNode	 boson.Address
	neighborNode boson.Address
}

func (s *ACOService)getAcoRoutes(route_list []TargetNeighborRoute, route_count int) ([]TargetNeighborRoute, error){
	return nil, nil
}

// func rankRouteDownload(route_list []TargetNeighborRoute, download_rate []float64)
func (s *ACOService)randkRouteDownload(route TargetNeighborRoute, download_rate int){}


// func ACONew(addr boson.Address, storer storage.Storer, streamer p2p.Streamer, chunkPeerer topology.EachPeerer, logger logging.Logger, tracer *tracing.Tracer) *Service {
// 	return &Service{
// 		addr:          addr,
// 		streamer:      streamer,
// 		peerSuggester: chunkPeerer,
// 		storer:        storer,
// 		logger:        logger,
// 		//accounting:    accounting,
// 		//pricer:        pricer,
// 		metrics: newMetrics(),
// 		tracer:  tracer,
// 	}
// }


func (s *ACOService) RetrieveChunk(ctx context.Context, root_addr, chunk_addr boson.Address) (boson.Chunk, error){
	// flight_key := fmt.Sprintf("%v:%v", root_addr.String, cid_addr.String())

	// v, err, _ := s.singleflight.Do(flight_key, func()(interface{}, error){

	// })
	// func (ci *ChunkInfo) GetChunkInfo(rootCid boson.Address, cid boson.Address) [][]byte {
	chunk_result := s.chunkinfo.GetChunkInfo(root_addr, chunk_addr)
	node_list := make([]boson.Address, 0)
	for _, v := range chunk_result{
		new_node := boson.NewAddress(v)
		node_list = append(node_list, new_node)
	}

	all_tn_route_list := make([]TargetNeighborRoute, 0)
	for _, node := range node_list{
		target_node := node
		route_item_list, err := s.routetabService.FindRoute(ctx, target_node)
		if err != nil{
			for _, v := range route_item_list{
				neighbor_node := v.Neighbor
				new_route := TargetNeighborRoute{target_node, neighbor_node}
				all_tn_route_list = append(all_tn_route_list, new_route)
			}
		}
	}

	var (
		routeAttemp int = 0
		maxAttemp int = 5
		resultC = make(chan boson.Chunk, maxAttemp)
		errC = make(chan error, maxAttemp)
	)

	aco_route_list, err := s.getAcoRoutes(all_tn_route_list, routeAttemp)
	if err != nil{
		return nil, fmt.Errorf("get aco route error: %v", err)
	}

	for select_route := range aco_route_list{
		go func(ctx context.Context){
			// select_route := route
			chunk, peer, err := s.retrieveChunck(ctx, select_route, root_addr, chunk_addr)

			select{
			case resultC <- chunk:
			case errC <- err:
			case <- ctx.Done():
				return
			}

		}(ctx)
	}


	// for{
	// 	if routeAttemp < maxAttemp{
	// 		routeAttemp ++
	// 		go func(ctx context.Context){
	// 			chunk, err := s.retrieveChunck(ctx, select_route, root_addr, chunck_addr)
	// 			if err != nil{
	// 				errC <- err
	// 				return
	// 			}
				
	// 			resultC <- chunk
	// 		}(ctx)
	// 	}
	// }

	var(
		chunk boson.Chunk
		// err error = error.Error()
		// err error
		err_count int = 0
	)

	for err_count < maxAttemp {
		select{
		case result:= <-resultC:
			break
		case <-errC:
			err = nil
			err_count += 1
		}
	}

	// return nil, nil

	return chunk, err
}

// ? 实际向邻接点发送请求是怎么实现的
func (s *ACOService) retrieveChunk(ctx context.Context, select_route TargetNeighborRoute, root_addr, chunk_addr boson.Address) (boson.Chunk, boson.Address, error){
	return nil, boson.ZeroAddress, nil
}
