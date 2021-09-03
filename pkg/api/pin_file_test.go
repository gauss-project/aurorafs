package api_test

import (
	"bytes"
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
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

func TestPinFilesHandler(t *testing.T) {

	var (
		fileUploadResource      = "/v1/files"
		pinFilesResource        = "/v1/pin/files"
		pinFilesAddressResource = func(addr string) string { return pinFilesResource + "/" + addr }

		simpleData  = []byte("this is a simple text")
		simpleData1 = []byte("this is a simple text1")

		mockStorer       = mock.NewStorer()
		traversalService = traversal.NewService(mockStorer)

		chunkinfo = func() *chunkinfo.ChunkInfo {
			logger := logging.New(ioutil.Discard, 0)
			serverAddress := boson.MustParseHexAddress("01")
			recorder := streamtest.New(
				streamtest.WithBaseAddr(serverAddress),
			)
			chuninfo := chunkinfo.New(recorder, logger, traversalService, "127.0.0.1:8000")
			return chuninfo
		}

		client, _, _ = newTestServer(t, testServerOptions{
			Storer:    mockStorer,
			Traversal: traversalService,
			ChunkInfo: chunkinfo(),
		})
	)

	rootHash := "741e2927a51c7b49069ab465a7c59a0fc8cdf82093ddd8353d9876596e0ff782"
	rootHash1 := "7d984c20b72385bc3d85922a73dac3d76c88fb9a3a37fffbfce58ab49b502a8f"

	//upload file
	err := fileUpAndPin(t, client, fileUploadResource, pinFilesAddressResource, simpleData, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	err = fileUpAndPin(t, client, fileUploadResource, pinFilesAddressResource, simpleData1, rootHash1)
	if err != nil {
		t.Fatal(err)
	}

	//pin status check
	ctx := context.Background()
	addr := boson.MustParseHexAddress(rootHash)
	addr1 := boson.MustParseHexAddress(rootHash1)

	var addlist, addlist1 []boson.Address

	getChunkList := func(address boson.Address) error {
		addlist = append(addlist, address)
		return nil
	}
	err = traversalService.TraverseFileAddresses(ctx, addr, getChunkList)
	if err != nil {
		t.Fatal(err)
	}

	getChunkList1 := func(address boson.Address) error {
		addlist1 = append(addlist1, address)
		return nil
	}
	err = traversalService.TraverseFileAddresses(ctx, addr1, getChunkList1)
	if err != nil {
		t.Fatal(err)
	}

	status := checkpin(addlist, mockStorer)
	if status == 0 {
		t.Fatal("pin check error")
	}

	status = checkpin(addlist1, mockStorer)
	if status == 0 {
		t.Fatal("pin check error")
	}

	err = fileUnpin(t, client, pinFilesAddressResource, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	status = checkpin(addlist, mockStorer)
	if status == 1 {
		t.Fatal("pin check error")
	}

	status = checkpin(addlist1, mockStorer)
	if status == 0 {
		t.Fatal("pin check error")
	}

}

func fileUpAndPin(t *testing.T, client *http.Client, fileUploadResource string, pinFilesAddressResource func(addr string) string, simpleData []byte, rootHash string) error {
	//upload file
	jsonhttptest.Request(t, client, http.MethodPost, fileUploadResource, http.StatusOK,
		jsonhttptest.WithRequestBody(bytes.NewReader(simpleData)),
		jsonhttptest.WithExpectedJSONResponse(api.FileUploadResponse{
			Reference: boson.MustParseHexAddress(rootHash),
		}),
		jsonhttptest.WithRequestHeader("Content-Type", "text/plain"),
	)

	//pin live file
	jsonhttptest.Request(t, client, http.MethodPost, pinFilesAddressResource(rootHash), http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: http.StatusText(http.StatusOK),
			Code:    http.StatusOK,
		}),
	)
	return nil
}

func fileUnpin(t *testing.T, client *http.Client, pinFilesAddressResource func(addr string) string, rootHash string) error {
	jsonhttptest.Request(t, client, http.MethodDelete, pinFilesAddressResource(rootHash), http.StatusOK,
		jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: http.StatusText(http.StatusOK),
			Code:    http.StatusOK,
		}),
	)
	return nil
}

func checkpin(addlist []boson.Address, mockStorer *mock.MockStorer) int {
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
