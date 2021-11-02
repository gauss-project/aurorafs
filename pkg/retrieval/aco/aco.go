package aco

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

const (
	defaultRate int64 = 2_000_000 / 8
	cleanTime         = 10 * time.Minute
)

type Route struct {
	LinkNode   boson.Address
	TargetNode boson.Address
}

func NewRoute(linkAddr boson.Address, targetAddr boson.Address) Route {
	return Route{
		LinkNode:   linkAddr,
		TargetNode: targetAddr,
	}
}

type DownloadDetail struct {
	StartMs int64
	EndMs   int64
	Size    int64
}

func (r *Route) ToString() string {
	return fmt.Sprintf("%v,%v", r.LinkNode.String(), r.TargetNode.String())
}

type routeMetric struct {
	downloadCount  int64
	downloadDetail *DownloadDetail
}

type AcoServer struct {
	routeMetric   map[string]*routeMetric
	toZeroElapsed int64
	mutex         sync.Mutex
}

func NewAcoServer() *AcoServer {
	aco := &AcoServer{
		routeMetric:   make(map[string]*routeMetric),
		toZeroElapsed: 20 * 60, // 1200s
	}
	aco.cleanTrigger()
	return aco
}

func (s *AcoServer) OnDownloadStart(route Route) {
	routeKey := route.ToString()

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exist := s.routeMetric[routeKey]; exist {
		s.routeMetric[routeKey].downloadCount += 1
	} else {
		s.routeMetric[routeKey] = &routeMetric{
			downloadCount: 1,
			downloadDetail: &DownloadDetail{
				0, 0, 0,
			},
		}
	}
}

func (s *AcoServer) onDownloadEnd(route Route) {
	routeKey := route.ToString()

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exist := s.routeMetric[routeKey]; exist {
		if s.routeMetric[routeKey].downloadCount > 0 {
			s.routeMetric[routeKey].downloadCount -= 1
		}
	}
}

func (s *AcoServer) OnDownloadEnd(route Route) {
	s.onDownloadEnd(route)
}

func (s *AcoServer) OnDownloadFinish(route Route, downloadDetail *DownloadDetail) {
	// if downloadDetail == nil{
	// 	s.onDownloadEnd(route)
	// 	return
	// }else{
	s.onDownloadTaskFinish(route, downloadDetail.StartMs, downloadDetail.EndMs, downloadDetail.Size)
	// }
}

func (s *AcoServer) onDownloadTaskFinish(route Route, startMs int64, endMs int64, size int64) {
	routeKey := route.ToString()
	var retStartMs, retEndMs int64

	if startMs < endMs {
		retStartMs, retEndMs = startMs, endMs
	} else if startMs == endMs {
		retStartMs, retEndMs = startMs, endMs+10
	} else {
		retStartMs, retEndMs = endMs, startMs
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	// new route, just record msg
	if _, exist := s.routeMetric[routeKey]; !exist {
		s.routeMetric[routeKey] = &routeMetric{
			downloadCount: 0,
			downloadDetail: &DownloadDetail{
				StartMs: retStartMs,
				EndMs:   retEndMs,
				Size:    size,
			},
		}
		// route exists, need update routeMetric
	} else {
		recordStartMs, recordEndMs := s.routeMetric[routeKey].downloadDetail.StartMs, s.routeMetric[routeKey].downloadDetail.EndMs

		if retEndMs < recordStartMs {
			return
		} else if recordEndMs < retStartMs {
			s.routeMetric[routeKey].downloadDetail = &DownloadDetail{
				StartMs: retStartMs,
				EndMs:   retEndMs,
				Size:    size,
			}
		} else {
			if retStartMs < recordStartMs {
				s.routeMetric[routeKey].downloadDetail.StartMs = retStartMs
			}
			if retEndMs > recordEndMs {
				s.routeMetric[routeKey].downloadDetail.EndMs = retEndMs
			}
			s.routeMetric[routeKey].downloadDetail.Size += size
		}
	}
}

func (s *AcoServer) GetRouteAcoIndex(routeList []Route, count ...int) []int {

	routeCount := len(routeList)
	if routeCount == 0 {
		return []int{}
	}

	maxSelectCount := routeCount
	if len(count) > 0 {
		setCount := count[0]
		if setCount < routeCount {
			maxSelectCount = setCount
		}
	}

	// get the score for each route
	routeScoreList := s.getSelectRouteListScore(routeList)

	// decide the order of the route
	routeIndexList := make([]int, 0)

	totalScore := int64(0)
	for _, v := range routeScoreList {
		totalScore += v
	}

	rand.Seed(time.Now().Unix())
	selectRouteCount := 0
	for {
		selectRouteIndex, curScore, curSum := 0, int64(0), int64(0)

		randNum := rand.Int63() % ((totalScore) + 1)
		for k, v := range routeScoreList {
			curScore = v
			if curScore == 0 {
				continue
			}
			nextSum := curSum + curScore
			if curSum < randNum && randNum <= nextSum {
				selectRouteIndex = k
				break
			}
			curSum = nextSum
		}

		routeIndexList = append(routeIndexList, selectRouteIndex)

		routeScoreList[selectRouteIndex] = 0
		totalScore -= curScore
		selectRouteCount += 1

		if selectRouteCount >= maxSelectCount {
			break
		}
	}
	return routeIndexList
}

func (s *AcoServer) getSelectRouteListScore(routeList []Route) []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	routeCount := len(routeList)
	routeScoreList := make([]int64, routeCount)

	for k, v := range routeList {
		curRoute := v
		curRouteScore := s.getCurRouteScore(curRoute)
		routeIndex := k
		routeScoreList[routeIndex] = curRouteScore
	}
	return routeScoreList
}

func (s *AcoServer) getCurRouteScore(route Route) int64 {
	routeKey := route.ToString()

	curRouteState, exist := s.routeMetric[routeKey]

	if !exist {
		return defaultRate
	}

	curUnixTs := time.Now().Unix()
	elapsed := curUnixTs - (curRouteState.downloadDetail.EndMs / 1000)
	if elapsed < 0 {
		elapsed = 0
	}

	if elapsed >= s.toZeroElapsed {
		return defaultRate / (curRouteState.downloadCount + 1)
	}

	if curRouteState.downloadDetail.EndMs == 0 {
		return defaultRate / (curRouteState.downloadCount + 1)
	}

	downloadDuration := float64(curRouteState.downloadDetail.EndMs-curRouteState.downloadDetail.StartMs) / 1000.
	downloadSize := float64(curRouteState.downloadDetail.Size)

	downloadRate := int64(downloadSize / downloadDuration)
	weightedDownloadRate := downloadRate / (curRouteState.downloadCount + 1)

	reserveScale := 1.0 - (float64(elapsed) / float64(s.toZeroElapsed))

	scoreAtCurrent := int64(float64(weightedDownloadRate-defaultRate)*reserveScale) + defaultRate

	return scoreAtCurrent
}

func (s *AcoServer) GetRouteScore(time int64) map[string]int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	res := make(map[string]int64)
	for k, v := range s.routeMetric {
		res[k] = s.getCur(time, v)
	}
	return res
}

func (s *AcoServer) getCur(t int64, curRouteState *routeMetric) int64 {

	if t == 0 {
		t = time.Now().Unix()
	}
	elapsed := t - (curRouteState.downloadDetail.EndMs / 1000)
	if elapsed < 0 {
		elapsed = 0
	}

	if elapsed >= s.toZeroElapsed {
		return defaultRate / (curRouteState.downloadCount + 1)
	}

	if curRouteState.downloadDetail.EndMs == 0 {
		return defaultRate / (curRouteState.downloadCount + 1)
	}

	downloadDuration := float64(curRouteState.downloadDetail.EndMs-curRouteState.downloadDetail.StartMs) / 1000.
	downloadSize := float64(curRouteState.downloadDetail.Size)

	downloadRate := int64(downloadSize / downloadDuration)
	weightedDownloadRate := downloadRate / (curRouteState.downloadCount + 1)

	reserveScale := 1.0 - (float64(elapsed) / float64(s.toZeroElapsed))

	scoreAtCurrent := int64(float64(weightedDownloadRate-defaultRate)*reserveScale) + defaultRate

	return scoreAtCurrent
}

func (s *AcoServer) cleanTrigger() {
	t := time.NewTicker(cleanTime)
	go func() {
		for {
			<-t.C
			now := time.Now().Unix()
			s.mutex.Lock()
			for k, v := range s.routeMetric {
				if v.downloadDetail.EndMs/1000+s.toZeroElapsed <= now {
					delete(s.routeMetric, k)
				}
			}
			s.mutex.Unlock()
		}
	}()
}
