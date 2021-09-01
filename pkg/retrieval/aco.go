package retrieval

import (
	// "fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

const (
	defaultRate int64 = 1_000_000
)

type route struct {
	linkNode boson.Address
	targetNode boson.Address
}

type routeKey struct {
	linkNodeStr 	string
	targetNodeStr	string
}

type downloadDetail struct{
	startMs int64
	endMs 	int64
	size	int64
}

type routeMetric struct{
	downloadCount	int64
	downloadDetail	*downloadDetail
}

type acoServer struct{
	routeMetric map[routeKey]*routeMetric
	toZeroElapsed int64
	mutex	sync.Mutex
}

func newAcoServer() *acoServer{
	return &acoServer{
		routeMetric: make(map[routeKey]*routeMetric),
		toZeroElapsed: 60*15,
		mutex: sync.Mutex{},
	}
}

func (s *acoServer) OnDownloadStart(route route){
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exist := s.routeMetric[routeKey]; exist{
		s.routeMetric[routeKey].downloadCount += 1
	}else{
		s.routeMetric[routeKey] = &routeMetric{
			downloadCount: 1,
			downloadDetail: &downloadDetail{
				0,0,0,
			},
		}
	}
}

func (s *acoServer) OnDownloadEnd(route route){
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exist := s.routeMetric[routeKey]; exist{
		if s.routeMetric[routeKey].downloadCount > 0{
			s.routeMetric[routeKey].downloadCount -= 1
		}
	}
}

func (s *acoServer) OnDownloadTaskFinish(route route, startMs int64, endMs int64, size int64){
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	// new route, just record msg
	if _, exist := s.routeMetric[routeKey]; !exist{
		s.routeMetric[routeKey] = &routeMetric{
			downloadCount: 0,
			downloadDetail: &downloadDetail{
				startMs: startMs,
				endMs: endMs,
				size: size,
			},
		}
	// route exists, need update routeMetric
	}else{
		recordStartMs, recordEndMs := s.routeMetric[routeKey].downloadDetail.startMs, s.routeMetric[routeKey].downloadDetail.endMs

		if endMs < recordStartMs{
			return
		}else if recordEndMs < startMs{
			s.routeMetric[routeKey].downloadDetail = &downloadDetail{
				startMs: startMs,
				endMs: endMs,
				size: size,
			}
		}else{
			if startMs < recordStartMs{
				s.routeMetric[routeKey].downloadDetail.startMs = startMs
			}
			if endMs > recordEndMs{
				s.routeMetric[routeKey].downloadDetail.endMs = endMs
			}
			s.routeMetric[routeKey].downloadDetail.size += size
		}
	}
}

func (s* acoServer) GetRouteAcoIndex(routeList []route) ([] int){
	routeCount := len(routeList)
	// get the score for each route
	routeScoreList := s.getSelectRouteListScore(routeList)

	// decide the order of the route 
	routeIndexList := make([]int, 0)

	totalScore := int64(0)
	for _, v := range routeScoreList{
		totalScore += v
	}

	rand.Seed(time.Now().Unix())
	selectRouteCount := 0
	for {
		curRouteIndex, curScore, curSum := 0, int64(0), int64(0)

		randNum := (rand.Int63()%(totalScore))+1
		for k, v := range routeScoreList{
			curScore = v
			if curScore == 0{
				continue
			}
			nextSum := curSum + curScore
			if curSum < randNum && randNum <= nextSum{
				curRouteIndex = k
				break
			} 
			curSum = nextSum
		}

		// fmt.Printf("%v, index: %v\n", randNum, curRouteIndex)
		routeIndexList = append(routeIndexList, curRouteIndex)
		routeScoreList[curRouteIndex] = 0
		totalScore -= curScore
		selectRouteCount += 1
		if selectRouteCount >= routeCount{
			break
		}
	}
	return routeIndexList
}

func (s *acoServer) GetRouteListScore(routeList []route)([]int64){
	return s.getSelectRouteListScore(routeList)
}

func (s *acoServer) getSelectRouteListScore(routeList []route)([]int64){
	routeCount := len(routeList)
	routeScoreList := make([]int64, routeCount)

	s.mutex.Lock()
	defer s.mutex.Unlock()
	for k, v := range routeList{
		curRoute := v
		curRouteScore := s.getCurRouteScore(curRoute)
		routeIndex := k
		routeScoreList[routeIndex] = curRouteScore
	}

	return routeScoreList
}

func (s *acoServer) getCurRouteScore(route route) int64{
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	curRouteState, exist := s.routeMetric[routeKey]

	if !exist{
		return defaultRate
	}

	curUnixTs := time.Now().Unix()
	elapsed := curUnixTs - (curRouteState.downloadDetail.endMs/1000)

	if elapsed >= s.toZeroElapsed{
		return defaultRate
	}

	downloadStartTs := float64(curRouteState.downloadDetail.startMs)/1000.0
	downloadEndTs := float64(curRouteState.downloadDetail.endMs)/1000.0

	downloadRate := int64(float64(curRouteState.downloadDetail.size)/(downloadEndTs-downloadStartTs))
	weightedDownloadRate := downloadRate/(curRouteState.downloadCount+1)
	reserveScale := 1.0 - (float64(elapsed)/float64(s.toZeroElapsed))

	scoreAtCurrent := int64(float64(weightedDownloadRate - defaultRate)*reserveScale) + defaultRate

	return scoreAtCurrent
}
