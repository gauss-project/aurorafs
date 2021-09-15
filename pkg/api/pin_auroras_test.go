package api_test

import (
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/p2p/streamtest"
	rmock "github.com/gauss-project/aurorafs/pkg/routetab/mock"
	statestore "github.com/gauss-project/aurorafs/pkg/statestore/mock"
	localstore "github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

func TestPinAuroras(t *testing.T) {
	var (
		dirUploadResource     = "/v1/aurora"
		pinBzzResource        = "/v1/pin/aurora"
		pinBzzAddressResource = func(addr string) string { return pinBzzResource + "/" + addr }

		mockStorer       = localstore.NewStorer()
		mockStateStorer  = statestore.NewStateStore()
		traversalService = traversal.NewService(mockStorer)
		mockRouteTable   = rmock.NewMockRouteTable()
		chunkinfo        = func() *chunkinfo.ChunkInfo {
			logger := logging.New(ioutil.Discard, 0)
			serverAddress := boson.MustParseHexAddress("01")
			recorder := streamtest.New(
				streamtest.WithBaseAddr(serverAddress),
			)
			chuninfo := chunkinfo.New(serverAddress, recorder, logger, traversalService, mockStateStorer, &mockRouteTable, "127.0.0.1:8000")
			return chuninfo
		}
		client, _, _ = newTestServer(t, testServerOptions{
			Storer:    mockStorer,
			Traversal: traversalService,
			ChunkInfo: chunkinfo(),
		})
	)

	files := []f{
		{
			data: []byte("<h1>Aurora"),
			name: "index.html",
			dir:  "",
		},
		{data: []byte("<h2>Aurora"),
			name: "user.html",
			dir:  "",
		},
	}
	files1 := []f{
		{
			data: []byte("<h1>Aurora"),
			name: "index.html",
			dir:  "",
		},
	}
	rootHash := "bbbda9652c7f616d5ffabd172721208c3980b2980dbd530225dae1cf79889c98"
	rootHash1 := "f879073d66de18fc6d94c6ce56e28e5a220498335d22df52e553965cbd405d9e"

	err := AurorasfileUpAndPin(t, client, dirUploadResource, pinBzzAddressResource, files, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	err = AurorasfileUpAndPin(t, client, dirUploadResource, pinBzzAddressResource, files1, rootHash1)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	addr := boson.MustParseHexAddress(rootHash)
	var addlist, addlist1 []boson.Address

	getChunkList := func(address boson.Address) error {
		addlist = append(addlist, address)
		return nil
	}
	err = traversalService.TraverseManifestAddresses(ctx, addr, getChunkList)
	if err != nil {
		t.Fatal(err)
	}

	addr1 := boson.MustParseHexAddress(rootHash1)
	getChunkList1 := func(address boson.Address) error {
		addlist1 = append(addlist1, address)
		return nil
	}
	err = traversalService.TraverseManifestAddresses(ctx, addr1, getChunkList1)
	if err != nil {
		t.Fatal(err)
	}

	chkstate := Aurorascheckpin(addlist, mockStorer)
	if chkstate == 0 {
		t.Fatal("pin is zero")
	}

	chkstate = Aurorascheckpin(addlist1, mockStorer)
	if chkstate == 0 {
		t.Fatal("pin is zero")
	}

	err = AurorasfileUnpin(t, client, pinBzzAddressResource, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	chkstate = Aurorascheckpin(addlist, mockStorer)
	if chkstate != 0 {
		t.Fatal("pin is exists")
	}

	chkstate = Aurorascheckpin(addlist1, mockStorer)
	if chkstate == 0 {
		t.Fatal("pin is exists")
	}

}

func AurorasfileUpAndPin(t *testing.T, client *http.Client, dirUploadResource string, pinBzzAddressResource func(addr string) string,
	files []f, rootHash string) error {

	tarReader := tarFiles(t, files)

	// verify directory tar upload response
	jsonhttptest.Request(t, client, http.MethodPost, dirUploadResource, http.StatusOK,
		jsonhttptest.WithRequestBody(tarReader),
		jsonhttptest.WithRequestHeader("Content-Type", api.ContentTypeTar),
		jsonhttptest.WithExpectedJSONResponse(api.FileUploadResponse{
			Reference: boson.MustParseHexAddress(rootHash),
		}),
	)

	//add pin to file
	jsonhttptest.Request(t, client, http.MethodPost, pinBzzAddressResource(rootHash), http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: http.StatusText(http.StatusOK),
			Code:    http.StatusOK,
		}),
	)

	return nil

}

func AurorasfileUnpin(t *testing.T, client *http.Client, pinBzzAddressResource func(addr string) string, rootHash string) error {
	jsonhttptest.Request(t, client, http.MethodDelete, pinBzzAddressResource(rootHash), http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: http.StatusText(http.StatusOK),
			Code:    http.StatusOK,
		}),
	)
	return nil
}

func Aurorascheckpin(addlist []boson.Address, mockStorer *localstore.MockStorer) int {
	iresult := 0
	for _, v := range addlist {
		icount, err := mockStorer.PinCounter(v)
		if err != nil {
			iresult = 0 //non-existend
			break
		} else {
			if icount > 0 {
				iresult = 1 //added pin
			} else {
				iresult = 0
				break
			}
		}
	}
	return iresult
}
