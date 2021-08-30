package retrieval

import (
	"time"
	"sync"
	"math/rand"
	"github.com/gauss-project/aurorafs/pkg/boson"
)

type routeItem struct{
	linkNode boson.Address
	targetNode boson.Address
}

type routeKey struct{
	linkNodeStr string
	targetNodeStr string
}

type downloadRecord struct{
	downloadRate 	int64
	downloadUnixTs 	int64
	usedCount		int64
}

const (
	defaultRate int64 = 625_000
)

type acoServer struct{
	routeRecord 	map[routeKey]downloadRecord
	// updateInterval	int64	// 30s
	toZeroElapsed	int64	// 1800s (30min)
	recordLock sync.Mutex
}

func (s *acoServer) getRouteAcoIndexList(routeList []routeItem) ([]int){
	routeCount := len(routeList)
	// get the score for each route
	routeScoreList := s.getSelectRouteListScore(routeList)

	// decide the order of the route 
	routeIndexList := make([]int, routeCount)

	totalScore := int64(0)
	for _, v := range routeScoreList{
		totalScore += v
	}

	selectRouteCount := 0
	for {
		// curSum := 0
		curRouteIndex, curScore, curSum := 0, int64(0), int64(0)
		for k, v := range routeScoreList{
			curScore := v
			if curScore == 0{
				continue
			}
			nextSum := curSum + curScore
			randNum := (rand.Int63()%(totalScore))+1
			if curSum < randNum && randNum <= nextSum{
				curRouteIndex = k
				break
			} 
			curSum = nextSum
		}

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

func (s *acoServer) getSelectRouteListScore(routeList []routeItem)([]int64){
	routeCount := len(routeList)
	routeScoreList := make([]int64, routeCount)
	// averageScore := s.getAverageScore()

	s.recordLock.Lock()
	defer s.recordLock.Unlock()
	for _, v := range routeList{
		curRoute := v
		curRouteScore := s.getCurRouteScore(curRoute)
		routeScoreList = append(routeScoreList, curRouteScore)
	}

	return routeScoreList
}

func (s *acoServer) getCurRouteScore(route routeItem) int64{
	recordKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	curRouteItem, exist := s.routeRecord[recordKey]

	if !exist{
		return defaultRate
	}

	curUnixTs := time.Now().Unix()
	elapsed := curUnixTs - curRouteItem.downloadUnixTs

	if elapsed >= s.toZeroElapsed{
		return defaultRate
	}

	downloadRate := curRouteItem.downloadRate
	initRate := downloadRate/(curRouteItem.usedCount+1)
	reserveScale := 1.0 - (float64(elapsed)/float64(s.toZeroElapsed))

	scoreAtMoment := int64(float64(initRate - defaultRate)*reserveScale) + defaultRate

	return scoreAtMoment
}

func (s *acoServer) onDownloadStart(route routeItem){
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	s.recordLock.Lock()
	defer s.recordLock.Unlock()
	routeRecord, exist := s.routeRecord[routeKey]
	if exist{
		newRouteRecord := routeRecord
		newRouteRecord.usedCount += 1
		s.routeRecord[routeKey] = newRouteRecord
	}
}

func (s *acoServer) onDownloadOver(route routeItem){
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}

	s.recordLock.Lock()
	defer s.recordLock.Unlock()
	routeRecord, exist := s.routeRecord[routeKey]
	if exist{
		if routeRecord.usedCount > 0{
			newRouteRecord := routeRecord
			newRouteRecord.usedCount -= 1
			s.routeRecord[routeKey] = newRouteRecord
		}
	}
}

func (s *acoServer) onChunkDownload(route routeItem, downloadRate int64){
	s.recordLock.Lock()
	defer s.recordLock.Unlock()
	curUnixTs := time.Now().Unix()
	routeKey := routeKey{
		linkNodeStr: route.linkNode.String(),
		targetNodeStr: route.targetNode.String(),
	}
	record, exist := s.routeRecord[routeKey]
	usedCount := 0
	if exist {
		usedCount = int(record.usedCount)
	}
	newDownloadRecord := downloadRecord{
		downloadRate: downloadRate,
		downloadUnixTs: curUnixTs,
		usedCount: int64(usedCount),
	}
	s.routeRecord[routeKey] = newDownloadRecord
}