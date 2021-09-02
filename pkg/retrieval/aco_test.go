package retrieval

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

func TestUsingCount(t *testing.T) {
	t.Run("OnDownloadStart and OnDownloadEnd", func(t *testing.T) {
		server := newAcoServer()
		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")

		route1 := route{
			linkNode:   addr1,
			targetNode: addr2,
		}

		route1Key := routeKey{
			linkNodeStr:   route1.linkNode.String(),
			targetNodeStr: route1.targetNode.String(),
		}

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
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode:   addr1,
			targetNode: addr2,
		}

		// new route, default score
		scoreList := server.GetRouteListScore([]route{route1})
		defaultScore := scoreList[0]
		t.Logf("default score: %v\n", defaultScore)
		if defaultScore != defaultRate {
			t.Fatalf("new route score error: want: %v, got: %v\n", defaultRate, defaultScore)
		}

		// time passed, default score, default passtime: 15*60s
		startTs := time.Now().Unix() - 30*60
		endTs := startTs + 5*60
		size := defaultScore * (endTs - startTs)
		server.OnDownloadTaskFinish(route1, startTs, endTs, int64(size))
		scoreList = server.GetRouteListScore([]route{route1})
		passtimeScore := scoreList[0]
		t.Logf("outdate  score: %v\n", passtimeScore)
		if passtimeScore != defaultRate {
			t.Fatalf("new route score error: want: %v, got: %v\n", passtimeScore, defaultScore)
		}

		// time not pass, default passtime: 15*60
		endTs = time.Now().Unix() - server.toZeroElapsed/2
		startTs = endTs - 5*60
		size = defaultScore * 1 * (endTs - startTs)

		timeFromEnd := time.Now().Unix() - endTs
		initScore := size / (endTs - startTs)
		wantScore := int64(float64(initScore-defaultScore)*(1.0-float64(timeFromEnd)/float64(server.toZeroElapsed))) + defaultScore

		server.OnDownloadTaskFinish(route1, startTs, endTs, int64(size))
		scoreList = server.GetRouteListScore([]route{route1})
		curScore := scoreList[0]
		t.Logf("curScore: %v, wantScore: %v\n", curScore, wantScore)
		if curScore != wantScore {
			t.Fatalf("Score error: want: %v, got: %v\n", passtimeScore, defaultScore)
		}
	})

	// multi download at same time
	t.Run("complex score test 01", func(t *testing.T) {
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode:   addr1,
			targetNode: addr2,
		}
		routeList := []route{route1}

		endMs := time.Now().UnixNano() / 1e6
		startMs := endMs - 5*60*1000
		size := defaultRate * 2 * (endMs - startMs)
		server.OnDownloadTaskFinish(route1, startMs, endMs, size)

		scoreList := server.GetRouteListScore(routeList)
		score0 := scoreList[0]
		t.Logf("0 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.GetRouteListScore(routeList)
		score1 := scoreList[0]
		t.Logf("1 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.GetRouteListScore(routeList)
		score2 := scoreList[0]
		t.Logf("2 downloading, route score: %v\n", scoreList[0])

		server.OnDownloadStart(route1)
		scoreList = server.GetRouteListScore(routeList)
		score3 := scoreList[0]
		t.Logf("3 downloading, route score: %v\n", scoreList[0])

		if !((score0 > score1) && (score1 > score2) && (score2 > score3)) {
			t.Fatalf("score error, want: 0: %v > 1: %v > 2:%v > 3:%v\n", score0, score1, score2, score3)
		}

		server.OnDownloadEnd(route1)
		scoreList = server.GetRouteListScore(routeList)
		score4 := scoreList[0]
		t.Logf("2 downloading, route score: %v\n", scoreList[0])

		if score4 != score2 {
			t.Fatalf("score error, want %v=%v\n", score4, score2)
		}
	})

	// overlap download score test
	t.Run("complex score test 02", func(t *testing.T) {
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode:   addr1,
			targetNode: addr2,
		}

		endMs := time.Now().UnixNano() / 1e6
		startMs := endMs - 5*60
		size := defaultRate * 2 * (endMs - startMs)

		overlapStartMs1, overlapStartMs2 := startMs, startMs
		overlapEndMs1, overlapEndMs2 := endMs, endMs
		server.OnDownloadTaskFinish(route1, overlapStartMs1, overlapEndMs1, size)
		server.OnDownloadTaskFinish(route1, overlapStartMs2, overlapEndMs2, size)

		scoreList := server.GetRouteListScore([]route{route1})
		curScore := scoreList[0]
		singleScore := defaultRate * 2
		t.Logf("overlap download 1:%v, 2: %v, sum(same time): %v", singleScore, singleScore, curScore)
		if curScore != (singleScore + singleScore) {
			t.Fatalf("overlap score error: want %v, got %v\n", curScore, (singleScore + singleScore))
		}

		route2 := route{
			linkNode:   addr2,
			targetNode: addr1,
		}
		endMs1 := time.Now().Unix() - 10*60
		startMs1 := endMs1 - 5*60
		endMs2 := time.Now().Unix()
		startMs2 := endMs2 - 5*60
		size1 := defaultRate * 3 * (endMs1 - startMs1)
		size2 := defaultRate * 2 * (endMs2 - startMs2)
		server.OnDownloadTaskFinish(route2, startMs1, endMs1, size1)
		server.OnDownloadTaskFinish(route2, startMs2, endMs2, size2)

		d1, d2 := defaultRate*3, defaultRate*2
		scoreList = server.GetRouteListScore([]route{route2})
		curScore = scoreList[0]
		t.Logf("non overlap download: d1: %v, d2: %v, resultScore: %v\n", d1, d2, curScore)
		if curScore != d2 {
			t.Fatalf("non overlap error: want: %v, got %v\n", d2, curScore)
		}
	})

	t.Run("complex score test 03", func(t *testing.T) {
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode:   addr1,
			targetNode: addr2,
		}

		taskCount := 20

		curTs := time.Now().Unix()

		mockRate := defaultRate * 2
		lastScore := defaultRate
		for i := 0; i < taskCount; i++ {
			endTs := curTs - int64(taskCount-1-i)*60
			startTs := endTs - 30
			size := mockRate * (endTs - startTs)
			server.OnDownloadTaskFinish(route1, startTs, endTs, size)
			scoreList := server.GetRouteListScore([]route{route1})
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

func TestAcoIndexOrder(t *testing.T) {
	acoServer := newAcoServer()

	downloadRate := []int64{1e6, 1e6, 2e6, 2e6, 3e6, 3e6, 4e6, 4e6, 5e6, 5e6}
	taskCount := len(downloadRate)
	routeList := make([]route, taskCount)

	for i := 0; i < taskCount; i++ {
		routeList[i] = route{
			linkNode:   boson.MustParseHexAddress(fmt.Sprintf("%02d", i)),
			targetNode: boson.MustParseHexAddress(fmt.Sprintf("%02d", i)),
		}
	}

	nano := time.Now().UnixNano()
	rand.Seed(nano)

	// fmt.Printf("%v: %v\n", nano, rand.Int31())

	for i := 0; i < taskCount; i++ {
		endMs := time.Now().UnixNano()/1e6	//+ 60*1000
		startMs := endMs - 5*60*1e3
		size := downloadRate[i] / 1e3 * (endMs - startMs)
		// fmt.Printf("size: %v\n", size)
		acoServer.OnDownloadTaskFinish(routeList[i], startMs, endMs, size)
	}

	// scoreList := acoServer.getSelectRouteListScore(routeList)
	// for _, v := range scoreList {
	// 	fmt.Printf("%v\n", v)
	// }

	indexStatic := make([]int, len(routeList))

	var checkRoute = 0
	for i:=0; i<1000;i++{
		indexList := acoServer.GetRouteAcoIndex(routeList)
		for k,v := range indexList{
			if v == checkRoute{
				indexStatic[k] += 1
			}
		}
	}
	fmt.Printf("0: %v\n", indexStatic)

	indexStatic = make([]int, len(routeList))
	checkRoute = 1
	for i:=0; i<1000;i++{
		indexList := acoServer.GetRouteAcoIndex(routeList)
		for k,v := range indexList{
			if v == checkRoute{
				indexStatic[k] += 1
			}
		}
	}
	fmt.Printf("1: %v\n", indexStatic)

	scoreList := acoServer.getSelectRouteListScore(routeList)
	fmt.Printf("%v\n", scoreList)
	// for _, v := range scoreList {
	// 	fmt.Printf("%v\n", v)
	// }
}

func TestAcoSortAlgrithm(t *testing.T){
	backupList := []int{10, 10, 10, 10, 10, 10, 10, 10}
	// var selectIndex int

	selectIndex := 4
	backupList[selectIndex] = 30
	fmt.Printf("%v\n", backupList)
	resultList := make([]int, len(backupList))

	// count := 0
	rand.Seed(time.Now().Unix())
	staticCount := 30000
	for i:=0;i<staticCount;i++{
		randNum := rand.Intn(100)+1
		curSum := 0
		var acoIndex int
		for k,v := range backupList{
			nextSum := curSum + v
			if curSum < randNum && randNum <= nextSum{
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

	acoServer := newAcoServer()
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
	// fmt.Printf("bandwidth static: \n%v\n", bandwidthStatic)
	t.Logf("bandwidth static: \n%v\n", bandwidthStatic)
	// routeScoreStatic := generateRouteMtricReport(metricRecord)
	// fmt.Printf("init metric static: \n%v\n", routeScoreStatic)
	// fmt.Println()

	for {
		fmt.Print(".")
		if mockClient.getFilledChunkCount() >= chunkCount {
			time.Sleep(100 * time.Millisecond)
			break
		}

		chunkId := mockClient.GetNextUnfilledChunkId()
		if chunkId != -1 {
			go func() {
				// t.Logf("chunk id: %v\n", chunkId)
				routeListForChunk := mockNet.getRouteListByChunkId(chunkId)

				acoRouteIndexList := acoServer.GetRouteAcoIndex(routeListForChunk)

				for _, routeIndex := range acoRouteIndexList {
					targetRoute := routeListForChunk[routeIndex]
					routeBandwidth := mockNet.queryRouteBandwidth(targetRoute)

					startMs := time.Now().UnixNano()/1e6
					acoServer.OnDownloadStart(targetRoute)
					// t.Logf("try <<-- cid: %03d from : %03d\n", chunkId, routeIndex)
					downloadResult := mockClient.TryDownloadChunk(targetRoute, chunkId, routeBandwidth)
					acoServer.OnDownloadEnd(targetRoute)
					if downloadResult {
						endMs := time.Now().UnixNano()/1e6

						acoServer.OnDownloadTaskFinish(targetRoute, startMs, endMs, chunkSize)
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

func recordClientDownloadRate(ctx context.Context, client *mockClient){
	for{
		client.mutex.Lock()
		fmt.Printf("%v B/s\n", client.usedBandwidth)
		client.mutex.Unlock()

		time.Sleep(300*time.Millisecond)

		select{
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

func recordAcoMetricSnap(ctx context.Context, routeList []route, acoServer *acoServer, metricRecordMap map[int][]int64) {
	i := 0
	step := 10

	for {
		fmt.Print("&")
		i+=1

		scoreList := acoServer.GetRouteListScore(routeList)
		for k,v := range scoreList{
			routeIndex := k
			curRouteScore := v
			metricRecordMap[routeIndex] = append(metricRecordMap[routeIndex], curRouteScore)
		}


		time.Sleep(100 * time.Millisecond)
		if i>step{
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
	routeList          []route
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

	routeList := make([]route, routeCount)
	for i := 0; i < routeCount; i++ {
		routeList[i].linkNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
		routeList[i].targetNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
	}

	return mockNet{
		routeCount:         routeCount,
		chunkCount:         chunkCount,
		routeBandwidthList: routeBandwidthList,
		routeList:          routeList,
		routeChunkBitmap:   routeChunkBitmap,
	}
}

func (n *mockNet) queryRouteBandwidth(route route) int64 {
	routeIndex := -1
	for i := 0; i< n.routeCount; i++{
		if n.routeList[i].linkNode.Equal(route.linkNode) && n.routeList[i].targetNode.Equal(route.targetNode) {
			routeIndex = i
			break
		}
	}

	return n.routeBandwidthList[routeIndex]
}

func (n *mockNet) getAllRouteList() []route {
	return n.routeList
}

func (n *mockNet) getRouteListByChunkId(chunkId int) []route {
	routeIndexList := make([]int, 0)

	for routeIndex := 0; routeIndex < n.routeCount; routeIndex++ {
		v := n.routeChunkBitmap[routeIndex][chunkId]
		if v == 1 {
			routeIndexList = append(routeIndexList, routeIndex)
		}
	}

	routeList := make([]route, len(routeIndexList))
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

func (c *mockClient) TryDownloadChunk(route route, chunkId int, routeBandwidth int64) bool {
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

func (c *mockClient) isUsingThisRoute(route route) bool {
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
	if v, exist := c.usingRouteMap[routeKey]; exist {
		return v
	} else {
		return false
	}
}

func (c *mockClient) appendUsingRoute(route route) {
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
	c.usingRouteMap[routeKey] = true
}

func (c *mockClient) deleteUsingRoute(route route) {
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
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