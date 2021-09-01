package retrieval

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	"context"

	"github.com/gauss-project/aurorafs/pkg/boson"
)

func TestUsingCount(t *testing.T) {
	t.Run("OnDownloadStart and OnDownloadEnd", func(t *testing.T){
		server := newAcoServer()
		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")

		route1 := route{
			linkNode: addr1,
			targetNode: addr2,
		}

		route1Key := routeKey{
			linkNodeStr: route1.linkNode.String(),
			targetNodeStr: route1.targetNode.String(),
		}

		// download start
		randNum := rand.Int63()%10+5
		wg := sync.WaitGroup{}
		// wg.Add(int(randNum))
		for i:=int64(0); i<randNum;i++{
			wg.Add(1)
			go func(){
				server.OnDownloadStart(route1)
				wg.Done()
			}()
		}
		wg.Wait()

		usingCount := server.routeMetric[route1Key].downloadCount
		if usingCount != randNum{
			t.Fatalf("OnDownloadStart Error except: %v, got: %v\n", randNum, usingCount)
		}else{
			t.Logf("except: %v, got: %v \n", randNum, usingCount)
		}

		for i:=int64(0); i<randNum;i++{
			wg.Add(1)
			go func(){
				server.OnDownloadEnd(route1)
				wg.Done()
			}()
		}
		wg.Wait()

		usingCount = server.routeMetric[route1Key].downloadCount
		if usingCount != 0{
			t.Fatalf("OnDownloadEnd Error except: %v, got: %v\n", 0, usingCount)
		}else{
			t.Logf("except: %v, got: %v \n", 0, usingCount)
		}
	})

	// t.Run("OnDownloadEnd", func(t *testing.T){
	// })
}

func TestRouteScore(t *testing.T){
	// existed routes score test
	t.Run("simple score test", func(t *testing.T){
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode: addr1,
			targetNode: addr2,
		}

		// new route, default score
		scoreList := server.GetRouteListScore([]route{route1})
		defaultScore := scoreList[0]
		t.Logf("default score: %v\n", defaultScore)
		if defaultScore != defaultRate{
			t.Fatalf("new route score error: want: %v, got: %v\n", defaultRate, defaultScore)
		}

		// time passed, default score, default passtime: 15*60s
		startTs := time.Now().Unix() - 30*60
		endTs := startTs + 5*60
		size := defaultScore * (endTs-startTs)
		server.OnDownloadTaskFinish(route1, startTs, endTs, int64(size))
		scoreList = server.GetRouteListScore([]route{route1})
		passtimeScore := scoreList[0]
		t.Logf("outdate  score: %v\n", passtimeScore)
		if passtimeScore != defaultRate{
			t.Fatalf("new route score error: want: %v, got: %v\n", passtimeScore, defaultScore)
		}

		// time not pass, default passtime: 15*60
		endTs = time.Now().Unix() - server.toZeroElapsed/2
		startTs = endTs - 5*60
		size = defaultScore*1 * (endTs-startTs)
		
		timeFromEnd := time.Now().Unix()-endTs
		initScore := size/(endTs-startTs)
		wantScore := int64(float64(initScore - defaultScore)*(1.0 - float64(timeFromEnd)/float64(server.toZeroElapsed))) + defaultScore

		server.OnDownloadTaskFinish(route1, startTs, endTs, int64(size))
		scoreList = server.GetRouteListScore([]route{route1})
		curScore := scoreList[0]
		t.Logf("curScore: %v, wantScore: %v\n", curScore, wantScore)
		if curScore != wantScore{
			t.Fatalf("Score error: want: %v, got: %v\n", passtimeScore, defaultScore)
		}
	})

	// multi download at same time
	t.Run("complex score test 01", func (t *testing.T){
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode: addr1,
			targetNode: addr2,
		}
		routeList := []route{route1}


		endTs := time.Now().Unix()
		startTs := endTs - 5*60
		size := defaultRate*2*(endTs-startTs)
		server.OnDownloadTaskFinish(route1, startTs, endTs, size)

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

		if !( (score0>score1) && (score1>score2) && (score2 > score3)){
			t.Fatalf("score error, want: 0: %v > 1: %v > 2:%v > 3:%v\n", score0, score1, score2, score3)
		}

		server.OnDownloadEnd(route1)
		scoreList = server.GetRouteListScore(routeList)
		score4 := scoreList[0]
		t.Logf("2 downloading, route score: %v\n", scoreList[0])

		if score4 != score2{
			t.Fatalf("score error, want %v=%v\n", score4, score2)
		}
	})

	// overlap download score test
	t.Run("complex score test 02", func (t *testing.T){
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode: addr1,
			targetNode: addr2,
		}

		endTs := time.Now().Unix()
		startTs := endTs - 5*60
		size := defaultRate*2*(endTs-startTs)

		overlapStartTs1, overlapStartTs2 := startTs, startTs
		overlapEndTs1, overlapEndTs2 := endTs, endTs
		server.OnDownloadTaskFinish(route1, overlapStartTs1, overlapEndTs1, size)
		server.OnDownloadTaskFinish(route1, overlapStartTs2, overlapEndTs2, size)

		scoreList := server.GetRouteListScore([]route{route1})
		curScore := scoreList[0]
		singleScore := defaultRate*2
		t.Logf("overlap download 1:%v, 2: %v, sum(same time): %v", singleScore, singleScore, curScore)
		if curScore != (singleScore + singleScore){
			t.Fatalf("overlap score error: want %v, got %v\n", curScore, (singleScore+singleScore))
		}

		route2 := route{
			linkNode: addr2,
			targetNode: addr1,
		}
		endTs1 := time.Now().Unix()-10*60
		startTs1 := endTs1 - 5*60
		endTs2 := time.Now().Unix()
		startTs2 := endTs2 - 5*60
		size1 := defaultRate*3*(endTs1-startTs1)
		size2 := defaultRate*2*(endTs2-startTs2)
		server.OnDownloadTaskFinish(route2, startTs1, endTs1, size1)
		server.OnDownloadTaskFinish(route2, startTs2, endTs2, size2)

		d1, d2 := defaultRate*3, defaultRate*2
		scoreList = server.GetRouteListScore([]route{route2})
		curScore = scoreList[0]
		t.Logf("non overlap download: d1: %v, d2: %v, resultScore: %v\n", d1, d2, curScore)
		if curScore != d2{
			t.Fatalf("non overlap error: want: %v, got %v\n", d2, curScore)
		}
	})

	t.Run("complex score test 03", func(t *testing.T){
		server := newAcoServer()

		addr1 := boson.MustParseHexAddress("01")
		addr2 := boson.MustParseHexAddress("02")
		// addr3 := boson.MustParseHexAddress("03")

		route1 := route{
			linkNode: addr1,
			targetNode: addr2,
		}

		taskCount := 20

		curTs := time.Now().Unix()

		mockRate := defaultRate*2
		lastScore := defaultRate
		for i:=0; i<taskCount; i++{
			endTs := curTs - int64(taskCount-1-i)*60
			startTs := endTs - 30
			size := mockRate*(endTs-startTs)
			server.OnDownloadTaskFinish(route1, startTs, endTs, size)
			scoreList := server.GetRouteListScore([]route{route1})
			curScore := scoreList[0]
			t.Logf("task %v: %v\n", i, curScore)

			if mockRate > defaultRate{
				if curScore < lastScore{
					t.Fatalf("cur score should less than last score: %v < %v\n", curScore, lastScore)
				}
			}else if mockRate < defaultRate{
				if curScore > lastScore{
					t.Fatalf("cur score should more than last score: %v > %v\n", curScore, lastScore)
				}
			}
			lastScore = curScore
		}
	})
}

func TestAcoEffectiveness(t *testing.T){

	acoServer := newAcoServer()
	chunkSize := CHUNK_SIZE

	routeCount, chunkCount := 10, 100
	mockNet := NewMockNet(routeCount, chunkCount)
	routeList := mockNet.getAllRouteList()

	mockClient := NewMockClient()
	mockClient.ConfigTask(mockNet)
	// bandwidthStatic := mockNet.getBandwidthStatic()

	ctx, cancel := context.WithCancel(context.Background())
	metricRecord := make(map[int][]int64)
	go recordAcoMetricSnap(ctx, routeList, acoServer, metricRecord)

	for{
		fmt.Print(".")
		if mockClient.getFilledChunkCount() >= chunkCount{
			time.Sleep(100*time.Millisecond)
			break
		}

		chunkId := mockClient.GetNextUnfilledChunkId()
		if chunkId != -1{
			go func(){
				// t.Logf("chunk id: %v\n", chunkId)
				fmt.Printf("cid: %v\n", chunkId)
				routeListForChunk := mockNet.getRouteListByChunkId(chunkId)
				acoRouteIndexList := acoServer.GetRouteAcoIndex(routeListForChunk)

				for _, routeIndex := range acoRouteIndexList {

					routeBandwidth := mockNet.queryRouteBandwidth(routeIndex)
					targetRoute := routeList[routeIndex]

					startMs := time.Now().UnixNano()/1e6
					acoServer.OnDownloadStart(targetRoute)
					// t.Logf("try <<-- cid: %03d from : %03d\n", chunkId, routeIndex)
					downloadResult := mockClient.TryDownloadChunk(targetRoute, chunkId, routeBandwidth)
					acoServer.OnDownloadEnd(targetRoute)
					if downloadResult{
						endMs := time.Now().UnixNano()/1e6

						acoServer.OnDownloadTaskFinish(targetRoute, startMs, endMs, chunkSize)
						elapsed := endMs - startMs
						// t.Logf("download chunk: %03d from : %03d in %d ms\n", chunkId, routeIndex, elapsed)
						fmt.Printf("download chunk: %03d, from: %02d in %d ms\n", chunkId, routeIndex, elapsed)
						break
					}else{
						// t.Logf("try failed cid: %03d from %03d\n", chunkId, routeIndex)
						// fmt.Printf("try failed cid: %03d from %03d\n", chunkId, routeIndex)
					}
				}
			}()
		}
		time.Sleep(100*time.Millisecond)
	}
	fmt.Println()

	cancel()

	routeScoreStatic:= generateRouteMtricReport(metricRecord)
	bandwidthStatic := mockNet.getBandwidthStatic()

	// t.Logf("route score static: %v\n", routeScoreStatic)
	// t.Logf("bandwidth static: %v]\n", bandwidthStatic)
	fmt.Printf("route score static: \n%v\n", routeScoreStatic)
	fmt.Printf("bandwidth static: \n%v\n", bandwidthStatic)
}

func generateRouteMtricReport(metricRecord map[int][]int64) string{

	scoreSumMap := make(map[int]int64)
	for k, v := range metricRecord{
		routeScoreSum := int64(0)
		for _, v1 := range v{
			curScore := v1
			routeScoreSum += curScore
		}

		routeIndex := k
		scoreSumMap[routeIndex] = routeScoreSum
	}

	routeCount := len(metricRecord)
	percentList := make([]float64, routeCount)

	totalScore := int64(0)
	for _,v := range scoreSumMap{
		curScore := v
		totalScore += curScore
	}

	for i:=0; i<routeCount;i++{
		curRouteScoreSum := scoreSumMap[i]
		percentList[i] = float64(curRouteScoreSum)/float64(totalScore)
	}
	
	percentListStr := ""
	for _, v := range percentList{
		curPercent := v
		percentListStr += fmt.Sprintf("%.2f%%, ", curPercent*100)
	}

	result := fmt.Sprintf("[%v]", percentListStr)
	return result
}

func recordAcoMetricSnap(ctx context.Context, routeList []route, acoServer *acoServer, metricRecordMap map[int][]int64){
	for{
		fmt.Print("&")
		for k, v := range routeList{
			curRoute := v
			curRouteScore := acoServer.getCurRouteScore(curRoute)

			routeIndex := k
			metricRecordMap[routeIndex] = append(metricRecordMap[routeIndex], curRouteScore)
		}

		time.Sleep(200*time.Millisecond)

		select{
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
	routeCount 			int
	chunkCount			int
	routeList			[]route
	routeBandwidthList	[]int64
	routeChunkMap		map[int][]int8
}

func NewMockNet(routeCount int, chunkCount int) mockNet{
	routeChunkMap := make(map[int][]int8)

	for i:=0; i<routeCount; i++{
		routeId := i
		routeChunkMap[routeId] = make([]int8, chunkCount)
	}
	halfRouteCount := routeCount/2

	rand.Seed(time.Now().Unix())
	for chunkIndex:=0; chunkIndex<chunkCount;chunkIndex++{
		filledCount := 0
		routeId := 0
		for {
			randNum := rand.Int31()%10
			if randNum >= 5{
				if routeChunkMap[routeId][chunkIndex] == 0{
					routeChunkMap[routeId][chunkIndex] = 1
					filledCount += 1
				}
			}
			if filledCount >= halfRouteCount{
				break
			}
			routeId += 1
			if routeId >= routeCount{
				routeId = 0
			}
		}
	}

	routeBandwidthList := generateRandomBandwidth(routeCount)

	routeList := make([]route, routeCount)
	for i:=0;i<routeCount;i++{
		routeList[i].linkNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
		routeList[i].targetNode = boson.MustParseHexAddress(fmt.Sprintf("%02d", i))
	}

	return mockNet{
		routeCount: routeCount,
		chunkCount: chunkCount,
		routeBandwidthList: routeBandwidthList,
		routeList: routeList,
		routeChunkMap: routeChunkMap,
	}
}

func (n *mockNet)queryRouteBandwidth(routeIndex int) int64{
	return n.routeBandwidthList[routeIndex]
}

func (n *mockNet)getAllRouteList() []route{
	return n.routeList
}

func (n *mockNet) getRouteListByChunkId(chunkId int) []route{
	routeIndexList := make([]int, 0)

	for routeIndex:=0; routeIndex<n.routeCount;routeIndex++{
		v := n.routeChunkMap[routeIndex][chunkId]
		if v == 1{
			routeIndexList= append(routeIndexList, routeIndex)
		}
	}

	routeList := make([]route, len(routeIndexList))
	for k,v := range routeIndexList{
		curRoute := n.routeList[v]
		routeList[k] = curRoute
	}

	return routeList

	// routeList := make([]route, 0)
	// for i:=0; i< n.routeCount; i++{
	// 	routeList = append(routeList, n.routeList[i])
	// }

	// return routeList
}

func (n *mockNet) getBandwidthStatic()string{
	percentList := make([]float64, n.routeCount)

	bandwidthSum := int64(0)
	for i:=0; i<n.routeCount; i++{
		bandwidth := n.routeBandwidthList[i]
		bandwidthSum += bandwidth
	}

	for i:=0; i<n.routeCount; i++{
		bandwidth := n.routeBandwidthList[i]
		percentValue := float64(bandwidth) / float64(bandwidthSum)
		percentList[i] = percentValue
	}

	listStr := ""
	for _,v := range percentList{
		percent := v
		listStr += fmt.Sprintf("%.2f%%, ", percent*100)
	}
	staticString := "[" + listStr + "]"
	return staticString
}

func generateRandomBandwidth(routeCount int) ([]int64){
	baseBandwidth := []int64{1_600_000, 1_800_000, 2_000_000, 2_200_000, 2_400_000, 2_600_000}
	baseBandwidthCount := len(baseBandwidth)
	bwList := make([]int64, routeCount)

	for i:=0; i<routeCount; i++{
		randIndex := rand.Int()%baseBandwidthCount
		randBandwidth := baseBandwidth[randIndex]
		bwList[i] = randBandwidth
	}
	return bwList
}

type mockClient struct {
	chunkStateList []int

	totalBandwidth int64
	usedBandwidth int64

	usingRouteMap map[string]bool
	mutex sync.Mutex
}

func NewMockClient() mockClient{
	return mockClient{
		totalBandwidth: 10_000_000,
		usedBandwidth: 0,
		usingRouteMap: make(map[string]bool),
		mutex: sync.Mutex{},
	}
}

func (c *mockClient) ConfigTask(net mockNet){
	c.chunkStateList = make([]int, net.chunkCount)

	for i:=0; i<net.chunkCount; i++{
		c.chunkStateList[i] = STATE_UNFILLED
	}
}

func (c *mockClient) TryDownloadChunk(route route, chunkId int, routeBandwidth int64) bool{
	if c.isUsingThisRoute(route){
		return false
	}

	c.mutex.Lock()
	restBandwidth := c.totalBandwidth-c.usedBandwidth
	// c.mutex.Unlock()

	if restBandwidth < routeBandwidth{
		c.mutex.Unlock()
		return false
	}

	// c.mutex.Lock()
	c.usedBandwidth += routeBandwidth
	c.appendUsingRoute(route)
	c.chunkStateList[chunkId] = STATE_DOWNLOADING
	c.mutex.Unlock()

	downloadTime := float64(CHUNK_SIZE)/float64(routeBandwidth) *1000.0*8.0
	// fmt.Printf("chunkId: %v, bandwidht: %v, time: %.2f ms\n", chunkId, routeBandwidth, downloadTime)
	waitDuration := time.Duration(int64(downloadTime))
	time.Sleep(waitDuration*time.Millisecond + 100*time.Millisecond)

	c.mutex.Lock()
	c.usedBandwidth -= routeBandwidth
	c.chunkStateList[chunkId] = STATE_FILLED
	c.deleteUsingRoute(route)
	c.mutex.Unlock()
	return true
}

func (c *mockClient) isUsingThisRoute(route route)bool{
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
	if v, exist := c.usingRouteMap[routeKey]; exist{
		return v
	}else{
		return false
	}
}

func (c *mockClient) appendUsingRoute(route route){
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
	c.usingRouteMap[routeKey] = true
}

func (c *mockClient) deleteUsingRoute(route route){
	routeKey := fmt.Sprintf("%v:%v", route.linkNode.String(), route.targetNode.String())
	c.usingRouteMap[routeKey] = false
}


func (c *mockClient) GetNextUnfilledChunkId() (int){
	chunkId := -1
	for k, v := range c.chunkStateList{
		if v == STATE_UNFILLED{
			chunkId = k
			break
		}
	}
	return chunkId
}

func (c *mockClient) getFilledChunkCount() int{
	c.mutex.Lock()
	defer c.mutex.Unlock()

	filledCount := 0
	for _,v := range c.chunkStateList{
		state := v
		if state == STATE_FILLED{
			filledCount += 1
		}
	}

	return filledCount
}