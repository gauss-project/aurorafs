package aco

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	// "github.com/gauss-project/aurorafs/pkg/retrieval/aco"
)

func TestUsingCount(t *testing.T) {
	t.Run("OnDownloadStart and OnDownloadEnd", func(t *testing.T) {
		server := NewAcoServer()
		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")

		route1 := Route{
			LinkNode:   addr1,
			TargetNode: addr2,
		}

		// route1Key := routeKey{
		// 	linkNodeStr:   route1.linkNode.String(),
		// 	targetNodeStr: route1.targetNode.String(),
		// }
		route1Key := route1.ToString()

		// download start
		randNum := rand.Int63()%10 + 5
		wg := sync.WaitGroup{}
		// wg.Add(int(randNum))
		for i := int64(0); i < randNum; i++ {
			wg.Add(1)
			go func() {
				server.OnDownloadStart(route1)
				wg.Done()
			}()
		}
		wg.Wait()

		usingCount := server.routeMetric[route1Key].downloadCount
		if usingCount != randNum {
			t.Fatalf("OnDownloadStart Error except: %v, got: %v\n", randNum, usingCount)
		} else {
			t.Logf("except: %v, got: %v \n", randNum, usingCount)
		}

		for i := int64(0); i < randNum; i++ {
			wg.Add(1)
			go func() {
				server.OnDownloadFinish(route1, &DownloadDetail{StartMs: 1, EndMs: 2, Size: CHUNK_SIZE})
				server.OnDownloadEnd(route1)
				wg.Done()
			}()
		}
		wg.Wait()

		usingCount = server.routeMetric[route1Key].downloadCount
		if usingCount != 0 {
			t.Fatalf("OnDownloadEnd Error except: %v, got: %v\n", 0, usingCount)
		} else {
			t.Logf("except: %v, got: %v \n", 0, usingCount)
		}
	})
}

func TestRouteScore(t *testing.T) {
	// existed routes score test
	t.Run("simple score test", func(t *testing.T) {
		server := NewAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := Route{
			LinkNode:   addr1,
			TargetNode: addr2,
		}

		// new route, default score
		scoreList := server.getSelectRouteListScore([]Route{route1})
		defaultScore := scoreList[0]
		t.Logf("default score: %v\n", defaultScore)
		if defaultScore != defaultRate {
			t.Fatalf("new route score error: want: %v, got: %v\n", defaultRate, defaultScore)
		}

		// time passed, default score, default passtime: 15*60s
		startMs := (time.Now().Unix() - 30*60) * 1000
		endMs := startMs + 5*60*1000
		size := defaultScore * ((endMs - startMs) / 1000)
		downloadDetail := DownloadDetail{startMs, endMs, size}
		server.OnDownloadFinish(route1, &downloadDetail)
		// server.OnDownloadTaskFinish(route1, startTs, endTs, int64(size))
		scoreList = server.getSelectRouteListScore([]Route{route1})
		passtimeScore := scoreList[0]
		t.Logf("outdate  score: %v\n", passtimeScore)
		if passtimeScore != defaultRate {
			t.Fatalf("new route score error: want: %v, got: %v\n", passtimeScore, defaultScore)
		}

		// time not pass, default passtime: 15*60
		route2 := Route{
			LinkNode:   addr1,
			TargetNode: addr1,
		}
		endMs = (time.Now().Unix() - server.toZeroElapsed/2) * 1000
		startMs = endMs - 5*60*1000
		size = defaultRate * 2 * ((endMs - startMs) / 1000)
		downloadDetail = DownloadDetail{startMs, endMs, size}
		server.OnDownloadFinish(route2, &downloadDetail)
		scoreList = server.getSelectRouteListScore([]Route{route2})
		curScore := scoreList[0]

		timeFromEnd := (time.Now().Unix()*1000 - endMs) / 1000
		initRate := size / ((endMs - startMs) / 1000)
		wantScore := int64(float64(initRate-defaultRate)*(1.0-float64(timeFromEnd)/float64(server.toZeroElapsed))) + defaultRate

		// server.OnDownloadTaskFinish(route1, startMs, endMs, int64(size))
		t.Logf("score caculate result: want: %v, got: %v\n", wantScore, curScore)
		if curScore != wantScore {
			t.Fatalf("Score error: want: %v, got: %v\n", wantScore, curScore)
		}
	})

	// multi download at same time
	t.Run("complex score test 01", func(t *testing.T) {
		server := NewAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := Route{
			LinkNode:   addr1,
			TargetNode: addr2,
		}
		routeList := []Route{route1}

		endMs := time.Now().UnixNano() / 1e6
		startMs := endMs - 5*60*1000
		size := defaultRate * 2 * (endMs - startMs)
		downloadDetail := DownloadDetail{startMs, endMs, size}
		server.OnDownloadFinish(route1, &downloadDetail)
		// server.OnDownloadTaskFinish(route1, startMs, endMs, size)

		scoreList := server.getSelectRouteListScore(routeList)
		score0 := scoreList[0]
		t.Logf("0 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.getSelectRouteListScore(routeList)
		score1 := scoreList[0]
		t.Logf("1 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.getSelectRouteListScore(routeList)
		score2 := scoreList[0]
		t.Logf("2 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.getSelectRouteListScore(routeList)
		score3 := scoreList[0]
		t.Logf("3 downloading, route score: %v\n", scoreList[0])

		if !((score0 > score1) && (score1 > score2) && (score2 > score3)) {
			t.Fatalf("score error, want: 0: %v > 1: %v > 2:%v > 3:%v\n", score0, score1, score2, score3)
		}

		server.onDownloadEnd(route1)
		scoreList = server.getSelectRouteListScore(routeList)
		score4 := scoreList[0]
		t.Logf("2 downloading, route score: %v\n", scoreList[0])

		if score4 != score2 {
			t.Fatalf("score error, want %v=%v\n", score4, score2)
		}
	})

	// overlap download score test
	t.Run("complex score test 02", func(t *testing.T) {
		server := NewAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := Route{
			LinkNode:   addr1,
			TargetNode: addr2,
		}

		endMs := time.Now().UnixNano() / 1e6
		startMs := endMs - 5*60*1000
		size := defaultRate * 2 * ((endMs - startMs) / 1000)

		overlapStartMs1, overlapStartMs2 := startMs, startMs
		overlapEndMs1, overlapEndMs2 := endMs, endMs
		server.OnDownloadFinish(route1, &DownloadDetail{overlapStartMs1, overlapEndMs1, size})
		server.OnDownloadFinish(route1, &DownloadDetail{overlapStartMs2, overlapEndMs2, size})

		scoreList := server.getSelectRouteListScore([]Route{route1})
		curScore := scoreList[0]
		singleScore := defaultRate * 2
		t.Logf("overlap download 1:%v, 2: %v, sum(same time): %v", singleScore, singleScore, curScore)
		if curScore != (singleScore + singleScore) {
			t.Fatalf("overlap score error: want %v, got %v\n", curScore, (singleScore + singleScore))
		}

		route2 := Route{
			LinkNode:   addr2,
			TargetNode: addr1,
		}
		endMs1 := (time.Now().Unix() - 10*60) * 1000
		startMs1 := endMs1 - 5*60*1000
		endMs2 := time.Now().Unix() * 1000
		startMs2 := endMs2 - 5*60*1000
		size1 := defaultRate * 3 * ((endMs1 - startMs1) / 1000)
		size2 := defaultRate * 2 * ((endMs2 - startMs2) / 1000)
		server.OnDownloadFinish(route2, &DownloadDetail{startMs1, endMs1, size1})
		server.OnDownloadFinish(route2, &DownloadDetail{startMs2, endMs2, size2})

		d1, d2 := defaultRate*3, defaultRate*2
		scoreList = server.getSelectRouteListScore([]Route{route2})
		curScore = scoreList[0]
		t.Logf("nonoverlap download: d1: %v, d2: %v, resultScore: %v\n", d1, d2, curScore)
		if curScore != d2 {
			t.Fatalf("nonoverlap error: want: %v, got %v\n", d2, curScore)
		}
	})

	// several non-overlap downloads
	t.Run("complex score test 03", func(t *testing.T) {
		server := NewAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := Route{
			LinkNode:   addr1,
			TargetNode: addr2,
		}

		taskCount := 20

		mockRate := defaultRate * 2
		lastScore := defaultRate
		for i := 0; i < taskCount; i++ {
			curMs := time.Now().UnixNano() / 1e6

			endMs := curMs - int64(taskCount-1-i)*60*1000
			startMs := endMs - 30*1000
			size := mockRate * ((endMs - startMs) / 1000)
			server.OnDownloadFinish(route1, &DownloadDetail{startMs, endMs, size})
			scoreList := server.getSelectRouteListScore([]Route{route1})
			curScore := scoreList[0]
			t.Logf("task %v: %v\n", i, curScore)

			if mockRate > defaultRate {
				if curScore < lastScore {
					t.Fatalf("cur score should less than last score: %v < %v\n", curScore, lastScore)
				}
			} else if mockRate < defaultRate {
				if curScore > lastScore {
					t.Fatalf("cur score should more than last score: %v > %v\n", curScore, lastScore)
				}
			}
			lastScore = curScore
		}
	})
}

func TestAcoSortAlgrithm(t *testing.T) {
	backupList := []int{10, 10, 10, 10, 10, 10, 10, 10}
	// var selectIndex int

	selectIndex := 4
	backupList[selectIndex] = 30
	fmt.Printf("%v\n", backupList)
	resultList := make([]int, len(backupList))

	// count := 0
	rand.Seed(time.Now().Unix())
	staticCount := 30000
	for i := 0; i < staticCount; i++ {
		randNum := rand.Intn(100) + 1
		curSum := 0
		var acoIndex int
		for k, v := range backupList {
			nextSum := curSum + v
			if curSum < randNum && randNum <= nextSum {
				acoIndex = k
				break
			}
			curSum = nextSum
		}
		resultList[acoIndex] += 1
	}
	fmt.Printf("%v\n", resultList)
}

func TestAcoEffectiveness(t *testing.T) {

	acoServer := NewAcoServer()
	chunkSize := CHUNK_SIZE

	routeCount, chunkCount := 10, 400
	mockNet := NewMockNet(routeCount, chunkCount)

	mockClient := NewMockClient()
	mockClient.ConfigTask(mockNet)

	ctx, cancel := context.WithCancel(context.Background())
	metricRecord := make(map[int][]int64)
	go recordAcoMetricSnap(ctx, mockNet.getAllRouteList(), acoServer, metricRecord)
	// go recordClientDownloadRate(ctx, &mockClient)

	bandwidthStatic := mockNet.getBandwidthStatic()
	fmt.Printf("bandwidth static: \n%v\n", bandwidthStatic)
	// routeScoreStatic := generateRouteMtricReport(metricRecord)
	// fmt.Printf("init metric static: \n%v\n", routeScoreStatic)
	// fmt.Println()

	for {
		fmt.Printf(".")
		if mockClient.getFilledChunkCount() >= chunkCount {
			time.Sleep(100 * time.Millisecond)
			break
		}

		chunkId := mockClient.GetNextUnfilledChunkId()
		if chunkId != -1 {
			go func() {
				// fmt.Printf("chunk id: %v\n", chunkId)
				routeListForChunk := mockNet.getRouteListByChunkId(chunkId)

				acoRouteIndexList := acoServer.GetRouteAcoIndex(routeListForChunk)

				for _, routeIndex := range acoRouteIndexList {
					targetRoute := routeListForChunk[routeIndex]
					routeBandwidth := mockNet.queryRouteBandwidth(targetRoute)

					startMs := time.Now().UnixNano() / 1e6
					acoServer.OnDownloadStart(targetRoute)
					// t.Logf("try <<-- cid: %03d from : %03d\n", chunkId, routeIndex)
					downloadResult := mockClient.TryDownloadChunk(targetRoute, chunkId, routeBandwidth)
					acoServer.onDownloadEnd(targetRoute)
					if downloadResult {
						endMs := time.Now().UnixNano() / 1e6

						downloadDetail := DownloadDetail{startMs, endMs, chunkSize}
						acoServer.OnDownloadFinish(targetRoute, &downloadDetail)

						// rate := int64(float64(chunkSize*1000*8)/float64(endMs-startMs))
						// duration := endMs - startMs
						// t.Logf("<: %v :%v, %dms\n", targetRoute.linkNode.String(), rate, duration)
						break
					}
				}
			}()
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println()

	cancel()
}

func generateRouteMtricReport(metricRecord map[int][]int64) string {

	scoreSumMap := make(map[int]int64)
	for k, v := range metricRecord {
		routeScoreSum := int64(0)
		for _, v1 := range v {
			curScore := v1
			routeScoreSum += curScore
		}

		routeIndex := k
		scoreSumMap[routeIndex] = routeScoreSum
	}

	routeCount := len(metricRecord)
	percentList := make([]float64, routeCount)

	totalScore := int64(0)
	for _, v := range scoreSumMap {
		curScore := v
		totalScore += curScore
	}

	for i := 0; i < routeCount; i++ {
		curRouteScoreSum := scoreSumMap[i]
		percentList[i] = float64(curRouteScoreSum) / float64(totalScore)
	}

	percentListStr := ""
	for _, v := range percentList {
		curPercent := v
		percentListStr += fmt.Sprintf("%.2f%%, ", curPercent*100)
	}

	result := fmt.Sprintf("[%v]", percentListStr)
	return result
}

func recordAcoMetricSnap(ctx context.Context, routeList []Route, acoServer *AcoServer, metricRecordMap map[int][]int64) {
	i := 0
	step := 10

	for {
		fmt.Print("&")
		i += 1

		scoreList := acoServer.getSelectRouteListScore(routeList)
		for k, v := range scoreList {
			routeIndex := k
			curRouteScore := v
			metricRecordMap[routeIndex] = append(metricRecordMap[routeIndex], curRouteScore)
		}

		time.Sleep(100 * time.Millisecond)
		if i > step {
			i = 0
			routeScoreStatic := generateRouteMtricReport(metricRecordMap)
			fmt.Printf("\n%v\n", routeScoreStatic)
			step = 30
			// fmt.Printf("route score static: \n%v\n", routeScoreStatic)
		}

		select {
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

const (
	STATE_UNFILLED int = iota
	STATE_DOWNLOADING
	STATE_FILLED
)

const CHUNK_SIZE int64 = 256_000

type mockNet struct {
	routeCount         int
	chunkCount         int
	routeList          []Route
	routeBandwidthList []int64
	routeChunkBitmap   [][]int8
}

func NewMockNet(routeCount int, chunkCount int) mockNet {
	// routeChunkMap := make(map[int][]int8)
	routeChunkBitmap := make([][]int8, routeCount)

	for i := 0; i < routeCount; i++ {
		routeId := i
		routeChunkBitmap[routeId] = make([]int8, chunkCount)
	}
	halfRouteCount := routeCount / 2

	rand.Seed(time.Now().Unix())
	for chunkIndex := 0; chunkIndex < chunkCount; chunkIndex++ {
		filledCount := 0
		for {
			// randRouteId := rand.Int() % routeCount
			randRouteId := rand.Intn(routeCount)
			if routeChunkBitmap[randRouteId][chunkIndex] == 0 {
				routeChunkBitmap[randRouteId][chunkIndex] = 1
				filledCount += 1
			}

			if filledCount >= halfRouteCount {
				break
			}
		}
	}

	routeBandwidthList := generateRandomBandwidth(routeCount)

	routeList := make([]Route, routeCount)
	for i := 0; i < routeCount; i++ {
		routeList[i].LinkNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
		routeList[i].TargetNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
	}

	return mockNet{
		routeCount:         routeCount,
		chunkCount:         chunkCount,
		routeBandwidthList: routeBandwidthList,
		routeList:          routeList,
		routeChunkBitmap:   routeChunkBitmap,
	}
}

func (n *mockNet) queryRouteBandwidth(route Route) int64 {
	routeIndex := -1
	for i := 0; i < n.routeCount; i++ {
		if n.routeList[i].LinkNode.Equal(route.LinkNode) && n.routeList[i].TargetNode.Equal(route.TargetNode) {
			routeIndex = i
			break
		}
	}

	return n.routeBandwidthList[routeIndex]
}

func (n *mockNet) getAllRouteList() []Route {
	return n.routeList
}

func (n *mockNet) getRouteListByChunkId(chunkId int) []Route {
	routeIndexList := make([]int, 0)

	for routeIndex := 0; routeIndex < n.routeCount; routeIndex++ {
		v := n.routeChunkBitmap[routeIndex][chunkId]
		if v == 1 {
			routeIndexList = append(routeIndexList, routeIndex)
		}
	}

	routeList := make([]Route, len(routeIndexList))
	for k, v := range routeIndexList {
		curRoute := n.routeList[v]
		routeList[k] = curRoute
	}

	return routeList
	// return n.routeList
}

func (n *mockNet) getBandwidthStatic() string {
	percentList := make([]float64, n.routeCount)

	bandwidthSum := int64(0)
	for i := 0; i < n.routeCount; i++ {
		bandwidth := n.routeBandwidthList[i]
		bandwidthSum += bandwidth
	}

	for i := 0; i < n.routeCount; i++ {
		bandwidth := n.routeBandwidthList[i]
		percentValue := float64(bandwidth) / float64(bandwidthSum)
		percentList[i] = percentValue
	}

	listStr := ""
	for _, v := range percentList {
		percent := v
		listStr += fmt.Sprintf("%.2f%%, ", percent*100)
	}
	staticString := "[" + listStr + "]"
	return staticString
}

func generateRandomBandwidth(routeCount int) []int64 {
	// baseBandwidth := []int64{1_600_000, 1_800_000, 2_000_000, 2_200_000, 2_400_000, 2_600_000}
	baseBandwidth := []int64{1_500_000, 2_000_000, 2_500_000}
	rand.Seed(2)
	baseBandwidthCount := len(baseBandwidth)
	bwList := make([]int64, routeCount)

	for i := 0; i < routeCount; i++ {
		randIndex := rand.Int() % baseBandwidthCount
		randBandwidth := baseBandwidth[randIndex]
		bwList[i] = randBandwidth
	}
	return bwList
	// return []int64{2_500_000, 2_500_000, 2_500_000, 2_500_000, 2_500_000, 1_500_000, 1_500_000, 1_500_000, 1_500_000, 1_500_000}
}

type mockClient struct {
	chunkStateList []int

	totalBandwidth int64
	usedBandwidth  int64

	usingRouteMap map[string]bool
	mutex         sync.Mutex
}

func NewMockClient() mockClient {
	return mockClient{
		totalBandwidth: 10_000_000,
		usedBandwidth:  0,
		usingRouteMap:  make(map[string]bool),
		mutex:          sync.Mutex{},
	}
}

func (c *mockClient) ConfigTask(net mockNet) {
	c.chunkStateList = make([]int, net.chunkCount)

	for i := 0; i < net.chunkCount; i++ {
		c.chunkStateList[i] = STATE_UNFILLED
	}
}

func (c *mockClient) TryDownloadChunk(route Route, chunkId int, routeBandwidth int64) bool {
	if c.isUsingThisRoute(route) {
		return false
	}

	c.mutex.Lock()
	restBandwidth := c.totalBandwidth - c.usedBandwidth

	if restBandwidth < routeBandwidth {
		c.mutex.Unlock()
		return false
	}

	c.usedBandwidth += routeBandwidth
	c.appendUsingRoute(route)
	c.chunkStateList[chunkId] = STATE_DOWNLOADING
	c.mutex.Unlock()

	downloadTime := float64(CHUNK_SIZE) / float64(routeBandwidth) * 1000.0 * 8.0
	// fmt.Printf("chunkId: %v, bandwidht: %v, time: %.2f ms\n", chunkId, routeBandwidth, downloadTime)
	waitDuration := time.Duration(int64(downloadTime))
	// time.Sleep(waitDuration*time.Millisecond)
	time.Sleep(waitDuration*time.Millisecond + 100*time.Millisecond)

	c.mutex.Lock()
	c.usedBandwidth -= routeBandwidth
	c.chunkStateList[chunkId] = STATE_FILLED
	c.deleteUsingRoute(route)
	c.mutex.Unlock()
	return true
}

func (c *mockClient) isUsingThisRoute(route Route) bool {
	routeKey := fmt.Sprintf("%v:%v", route.LinkNode.String(), route.TargetNode.String())
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if v, exist := c.usingRouteMap[routeKey]; exist {
		return v
	} else {
		return false
	}
}

func (c *mockClient) appendUsingRoute(route Route) {
	routeKey := fmt.Sprintf("%v:%v", route.LinkNode.String(), route.TargetNode.String())
	c.usingRouteMap[routeKey] = true
}

func (c *mockClient) deleteUsingRoute(route Route) {
	routeKey := fmt.Sprintf("%v:%v", route.LinkNode.String(), route.TargetNode.String())
	c.usingRouteMap[routeKey] = false
}

func (c *mockClient) GetNextUnfilledChunkId() int {
	chunkId := -1
	for k, v := range c.chunkStateList {
		if v == STATE_UNFILLED {
			chunkId = k
			break
		}
	}
	return chunkId
}

func (c *mockClient) getFilledChunkCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	filledCount := 0
	for _, v := range c.chunkStateList {
		state := v
		if state == STATE_FILLED {
			filledCount += 1
		}
	}

	return filledCount
}
