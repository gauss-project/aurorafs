package traversal_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

const fileContentType = "text/plain; charset=utf-8"

var enableLargeTest = false

func uploadFile(t *testing.T, ctx context.Context, store storage.Storer, file []byte, filename string, encrypted bool, ) (boson.Address, string) {
	pipe := builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(file), int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}

	if filename == "" {
		filename = fr.String()
	}

	m := entry.NewMetadata(filename)
	m.MimeType = fileContentType
	metadata, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	pipe = builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	mr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(metadata), int64(len(metadata)))
	if err != nil {
		t.Fatal(err)
	}

	entries := entry.New(fr, mr)
	entryBytes, err := entries.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	pipe = builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	reference, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(entryBytes), int64(len(entryBytes)))
	if err != nil {
		t.Fatal(reference)
	}

	return reference, filename
}

func uploadDirs(t *testing.T, ctx context.Context, store storage.Storer, manifestType string, files []file, encrypted bool) (boson.Address, [][]byte) {
	var (
		dirManifest manifest.Interface
		err         error
	)
	ls := loadsave.New(store, storage.ModePutRequest, encrypted)
	switch manifestType {
	case manifest.ManifestSimpleContentType:
		dirManifest, err = manifest.NewSimpleManifest(ls)
		if err != nil {
			t.Fatal(err)
		}
	case manifest.ManifestMantarayContentType:
		dirManifest, err = manifest.NewMantarayManifest(ls, encrypted)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("manifest: invalid type: %s", manifestType)
	}

	bytesFiles := make([][]byte, len(files))
	for i, f := range files {
		bytesData := generateSampleData(f.size)
		reference, filename := uploadFile(t, ctx, store, bytesData, f.name, encrypted)
		filePath := path.Join(f.dir, filename)
		err = dirManifest.Add(ctx, filePath, manifest.NewEntry(reference, nil))
		if err != nil {
			t.Fatal(err)
		}
		bytesFiles[i] = bytesData
	}

	manifestReference, err := dirManifest.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("manifest hash = %s\n", manifestReference)

	metadata := entry.NewMetadata(manifestReference.String())
	metadata.MimeType = dirManifest.Type()
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}

	pipe := builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	mr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(metadataBytes), int64(len(metadataBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("metadata hash = %s\n", mr)

	entries := entry.New(manifestReference, mr)
	entryBytes, err := entries.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	pipe = builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	reference, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(entryBytes), int64(len(entryBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reference hash = %s\n", reference)

	return reference, bytesFiles
}

func TestGetFileTrie(t *testing.T) {
	testCases := []struct {
		chunkSize           int
		expectedHashesCount int
		expectedHashes      []string
	}{
		{
			chunkSize:   boson.ChunkSize,
			expectedHashesCount: 3,
			expectedHashes: []string{
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // file
				"4d63a3450eb5adf0ec9941c2997b0adca6d90a7e8dc1f16cc5638dd527308225", // metadata
				"d22aa075a95cea77c8ce7e5f8777cb9e0c620b77a1f7372df87f5e45d7c678d1", // root
			},
		},
		{
			chunkSize:   boson.ChunkSize + 1,
			expectedHashesCount: 3,
			expectedHashes: []string{
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219", // file
				"9777abf8d3c05ee6c2eabd7fc68f214dea9b7ffe3976da0ace7953ff23bb69d2", // metadata
				"7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809", // root
			},
		},
	}

	for _, tc := range testCases {
		chunkCount := int(math.Ceil(float64(tc.chunkSize) / boson.ChunkSize))
		testName := fmt.Sprintf("%d-chunk-%d-bytes", chunkCount, tc.chunkSize)
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			bytesData := generateSampleData(tc.chunkSize)

			mockStore := mock.NewStorer()

			reference, _ := uploadFile(t, ctx, mockStore, bytesData, "", false)
			traversalService := traversal.NewService(mockStore)

			checkAddressFound := func(t *testing.T, foundAddresses map[string]struct{}, address string) {
				t.Helper()

				if _, ok := foundAddresses[address]; !ok {
					t.Fatalf("expected address %s not found", address)
				}
			}

			trieData, err := traversalService.GetTrieData(ctx, reference)
			if err != nil {
				t.Fatal(err)
			}

			if len(trieData) != tc.expectedHashesCount {
				t.Fatalf("expected to find %d addresses, got %d", tc.expectedHashesCount, len(trieData))
			}

			foundAddressMap := make(map[string]struct{})
			for existHash := range trieData {
				foundAddressMap[existHash] = struct{}{}
			}
			for _, expectedHash := range tc.expectedHashes {
				checkAddressFound(t, foundAddressMap, expectedHash)
			}
		})
	}
}

func TestCheckFileTrie(t *testing.T) {
	ctx := context.Background()
	bytesData := generateSampleData(boson.ChunkSize)

	mockStoreA := mock.NewStorer()
	mockStoreB := mock.NewStorer()

	reference, _ := uploadFile(t, ctx, mockStoreA, bytesData, "", false)
	traversalService := traversal.NewService(mockStoreA)

	trieData, err := traversalService.GetTrieData(ctx, reference)
	if err != nil {
		t.Fatal(err)
	}

	traversalService = traversal.NewService(mockStoreB)
	filesHashes, err := traversalService.CheckTrieData(ctx, reference, trieData)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.NewBuffer([]byte{})
	for _, f := range filesHashes {
		for index, h := range f {
			ch, err := mockStoreA.Get(ctx, storage.ModeGetRequest, boson.NewAddress(h))
			if err != nil {
				t.Fatalf("%d chunks: %v\n", index, err)
			}
			data.Write(ch.Data()[8:])
		}
	}

	cnt := len(bytesData) / boson.ChunkSize
	if len(bytesData) % boson.ChunkSize != 0 {
		cnt++
	}
	if len(filesHashes[0]) != cnt {
		t.Fatalf("expected to find %d chunks, got %d", cnt, len(filesHashes[0]))
	}

	if !bytes.Equal(data.Bytes(), bytesData) {
		t.Fatalf("received chunk hash incompleted")
	}
}

func TestGetLargeFileTrie(t *testing.T) {
	if !enableLargeTest {
		t.Skipf("disable large file test\n")
	}

	var (
		mockStoreA            *mock.MockStorer
		mockStoreB            *mock.MockStorer
		foundAddressForAMutex sync.Mutex
	)

	chunkCount := 8192 + 1
	expectedFileHashCount := 3
	expectedChunkHashCount := chunkCount

	ctx := context.Background()
	largeBytesData := generateSampleData(boson.ChunkSize * chunkCount)

	mockStoreA = mock.NewStorer()

	reference, _ := uploadFile(t, ctx, mockStoreA, largeBytesData, "", false)
	traversalService := traversal.NewService(mockStoreA)

	tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	foundAddressForACount := 0
	addressIterFn := func(addr boson.Address) error {
		foundAddressForAMutex.Lock()
		defer foundAddressForAMutex.Unlock()

		foundAddressForACount++
		return nil
	}
	err := traversalService.TraverseFileAddresses(tCtx, reference, addressIterFn)
	if err != nil {
		t.Fatal(err)
	}

	if foundAddressForACount != expectedChunkHashCount + expectedFileHashCount + 1 {
		t.Fatalf("expected to find %d chunk hash, got %d\n", expectedChunkHashCount + expectedFileHashCount + 1, foundAddressForACount)
	}

	mockStoreB = mock.NewStorer()

	trieData, err := traversalService.GetTrieData(ctx, reference)
	if err != nil {
		t.Fatal(err)
	}

	// one intermediate full level
	if len(trieData) != expectedFileHashCount + 1 {
		t.Fatalf("expected to find %d file hash(root/file/metadata), got %d\n", expectedFileHashCount + 1, len(trieData))
	}

	traversalService = traversal.NewService(mockStoreB)
	chunkHashes, err := traversalService.CheckTrieData(ctx, reference, trieData)
	if err != nil {
		t.Fatal(err)
	}

	// only a file
	file := chunkHashes[0]
	if len(file) != expectedChunkHashCount {
		t.Fatalf("expected to find %d chunk hash, got %d\n", expectedChunkHashCount, len(file))
	}
}

func TestGetDirsTrie(t *testing.T) {
	testCases := []struct{
		manifestType        string
		files               []file
		manifestHashes      []string
		expectedHashesCount int
	}{
		{
			manifestType: manifest.ManifestSimpleContentType,
			files: []file{
				{
					size: len(simpleData),
					dir:       "",
					name:      "hello.txt",
					reference: "82c00776b4c0ffbf67e79aeff71a76ee1b2416d70e46d30543d217982d935b50",
					chunks: fileChunks{
						metadata: "ec39f5a679d86230a863af6a54c395e29b077f24a8b925540a9ee3a770f6a822",
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
			},
			manifestHashes: []string{
				"6c0f20f4e792eb1be0fcba5693eb6878d6c17bb38ca05b7427f11c30975658d5", // manifest metadata
				"06597037b942a720696ed5bd109a8d7dbd4f02a5ab01a2a8fb77e2b3b66d51ec", // root
				"ac630a99d903c1ed60d32d08b53b1fd423f8a5de02f00530be21b47f44dcdeff",
			},
			expectedHashesCount: 6,
		},
		{
			manifestType: manifest.ManifestSimpleContentType,
			files: []file{
				{
					size: len(simpleData),
					dir:       "",
					name:      "hello.txt",
					reference: "82c00776b4c0ffbf67e79aeff71a76ee1b2416d70e46d30543d217982d935b50",
					chunks: fileChunks{
						metadata: "ec39f5a679d86230a863af6a54c395e29b077f24a8b925540a9ee3a770f6a822",
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
				{
					size: boson.ChunkSize,
					dir:       "",
					name:      "data/1.txt",
					reference: "12ea11034ef4b028d380fbc22b82b32890e3cdf4362d32a85cd621c8cc5c711e",
					chunks: fileChunks{
						metadata: "38a376fa1720f5814a197991b210f5b02f1b0e691467f09015d066068fb550ac",
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
						},
					},
				},
				{
					size: boson.ChunkSize + 1,
					dir:       "",
					name:      "data/2.txt",
					reference: "e38348132f2e937d10840e35d033da8b4320a5bdd2b96937b445e1d9913d236a",
					chunks: fileChunks{
						metadata: "b64e9d481a2c1e267f80fff87a67e58f50214a01396dc1729945bca87620cec6",
						content: []string{
							"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219",
						},
					},
				},
			},
			manifestHashes: []string{
				"b2e8dde6ee9cef21c655bbc65b82fbe6066f0c4ba15df39d4afb86bdd1a3ea6c", // manifest metadata
				"e38348132f2e937d10840e35d033da8b4320a5bdd2b96937b445e1d9913d236a", // root
				"3a50fef2d1fcf64a740529b446c13ce707fa7aaabeec4c6d688f4bb6fa003485",
			},
			expectedHashesCount: 12,
		},
		{
			manifestType: manifest.ManifestMantarayContentType,
			files: []file{
				{
					size:      len(simpleData),
					dir:       "",
					name:      "hello.txt",
					reference: "82c00776b4c0ffbf67e79aeff71a76ee1b2416d70e46d30543d217982d935b50",
					chunks: fileChunks{
						metadata: "ec39f5a679d86230a863af6a54c395e29b077f24a8b925540a9ee3a770f6a822",
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
			},
			manifestHashes: []string{
				"aa33cac9a6485bcc2dec0621dd9a6632656e614e3c3a15b167deec788a732ba3", // manifest metadata
				"26e36da1582136cbba2d098bba5cdb029ff90895ca423ad720a73923f00db2c6", // root
				"29bd351ee3ada85119c34f9070800352d5d27959d116498ba49ca4273fb62a3e",
				"185f1d1f53c07670dffa7adc4836156fe86c8b2e970c2614923a43e1e51e9f78",
			},
			expectedHashesCount: 7,
		},
		{
			manifestType: manifest.ManifestMantarayContentType,
			files: []file{
				{
					size:      len(simpleData),
					dir:       "",
					name:      "hello.txt",
					reference: "82c00776b4c0ffbf67e79aeff71a76ee1b2416d70e46d30543d217982d935b50",
					chunks: fileChunks{
						metadata: "ec39f5a679d86230a863af6a54c395e29b077f24a8b925540a9ee3a770f6a822",
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
				{
					size:      boson.ChunkSize,
					dir:       "",
					name:      "data/1.txt",
					reference: "12ea11034ef4b028d380fbc22b82b32890e3cdf4362d32a85cd621c8cc5c711e",
					chunks: fileChunks{
						metadata: "38a376fa1720f5814a197991b210f5b02f1b0e691467f09015d066068fb550ac",
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
						},
					},
				},
				{
					size:      boson.ChunkSize + 1,
					dir:       "",
					name:      "data/2.txt",
					reference: "e38348132f2e937d10840e35d033da8b4320a5bdd2b96937b445e1d9913d236a",
					chunks: fileChunks{
						metadata: "b64e9d481a2c1e267f80fff87a67e58f50214a01396dc1729945bca87620cec6",
						content: []string{
							"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219",
						},
					},
				},
			},
			manifestHashes: []string{
				"6aefbbf8463457cc4d75375f72d9f79a5d2ed1273e519c491b272f918516f4b3", // manifest
				"195079eed241575ab2432353a4a8fbbfcc587164dd87bf91bf86080752e7406e", // metadata
				"9beb5b55c9934622e58e72a29c94c6aeb821226a8f8bd0208955a17b6b4cba5c", // root
				"328d17e24f5314f111e9a4871da735e4b9a96ec3532b83d5858f5a1bdcb9e7ef",
				"4b959ecdfca8e4d017e9a9b8b4b309d6bbf24ed2973fcb821db5f172e334424c",
				"3fbd44b239a7bbec9dd82a4b0c909d1449b84cb0c73b7ba89390fda4136ab921",
				"185f1d1f53c07670dffa7adc4836156fe86c8b2e970c2614923a43e1e51e9f78",
			},
			expectedHashesCount: 16, // because same file content!!!
		},
	}

	for _, tc := range testCases {
		var testName string
		switch tc.manifestType {
		case manifest.ManifestSimpleContentType:
			testName = fmt.Sprintf("simple-%d-files-%d-chunks", len(tc.files), tc.expectedHashesCount)
		case manifest.ManifestMantarayContentType:
			testName = fmt.Sprintf("mantaray-%d-files-%d-chunks", len(tc.files), tc.expectedHashesCount)
		default:
			t.Fatalf("unknown manifest type: %s\n", tc.manifestType)
		}

		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			mockStore := mock.NewStorer()

			manifestReference, _ := uploadDirs(t, ctx, mockStore, tc.manifestType, tc.files, false)
			traversalService := traversal.NewService(mockStore)

			checkAddressFound := func(t *testing.T, foundAddresses map[string]struct{}, address string) {
				t.Helper()

				if _, ok := foundAddresses[address]; !ok {
					t.Fatalf("expected address %s not found", address)
				}
			}

			expectedHashes := make([]string, 0)
			for _, f := range tc.files {
				expectedHashes = append(expectedHashes, f.reference, f.chunks.metadata)
				if f.size <= boson.ChunkSize {
					expectedHashes = append(expectedHashes, f.chunks.content[0])
				}
			}
			expectedHashes = append(expectedHashes, tc.manifestHashes...)

			trieData, err := traversalService.GetTrieData(ctx, manifestReference)
			if err != nil {
				t.Fatal(err)
			}

			if len(trieData) != tc.expectedHashesCount {
				t.Fatalf("expected to find %d addresses, got %d", tc.expectedHashesCount, len(trieData))
			}

			foundAddressMap := make(map[string]struct{})
			for existHash := range trieData {
				foundAddressMap[existHash] = struct{}{}
			}

			for _, expectedHash := range expectedHashes {
				checkAddressFound(t, foundAddressMap, expectedHash)
			}
		})
	}
}

func TestCheckDirsTrie(t *testing.T) {
	testCases := []struct{
		manifestType        string
		files               []file
	}{
		{
			manifestType: manifest.ManifestSimpleContentType,
			files: []file{
				{
					size: len(simpleData),
					dir:       "",
					name:      "hello.txt",
				},
				{
					size: boson.ChunkSize + 1,
					dir:       "",
					name:      "data/1.txt",
				},
			},
		},
		{
			manifestType: manifest.ManifestMantarayContentType,
			files: []file{
				{
					size:      boson.ChunkSize * 2,
					dir:       "",
					name:      "hello.txt",
				},
			},
		},
	}

	for _, tc := range testCases {
		var testName string
		switch tc.manifestType {
		case manifest.ManifestSimpleContentType:
			testName = fmt.Sprintf("simple-%d-files", len(tc.files))
		case manifest.ManifestMantarayContentType:
			testName = fmt.Sprintf("mantaray-%d-files", len(tc.files))
		default:
			t.Fatalf("unknown manifest type: %s\n", tc.manifestType)
		}

		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()

			mockStoreA := mock.NewStorer()
			mockStoreB := mock.NewStorer()

			sort.Slice(tc.files, func(i, j int) bool {
				return tc.files[i].name < tc.files[j].name
			})

			manifestReference, bytesFiles := uploadDirs(t, ctx, mockStoreA, tc.manifestType, tc.files, false)
			traversalService := traversal.NewService(mockStoreA)

			trieData, err := traversalService.GetTrieData(ctx, manifestReference)
			if err != nil {
				t.Fatal(err)
			}

			traversalService = traversal.NewService(mockStoreB)
			filesHashes, err := traversalService.CheckTrieData(ctx, manifestReference, trieData)
			if err != nil {
				t.Fatal(err)
			}

			sort.Slice(filesHashes, func(i, j int) bool {
				return len(filesHashes[i]) > len(filesHashes[j])
			})

			data := bytes.NewBuffer([]byte{})
			for i, f := range filesHashes {
				for j, h := range f {
					ch, err := mockStoreA.Get(ctx, storage.ModeGetRequest, boson.NewAddress(h))
					if err != nil {
						t.Fatalf("%d chunks: %v\n", j, err)
					}
					data.Write(ch.Data()[8:])
				}

				if len(f) > 0 {
					cnt := tc.files[i].size / boson.ChunkSize
					if tc.files[i].size % boson.ChunkSize != 0 {
						cnt++
					}
					if len(filesHashes[i]) != cnt {
						t.Fatalf("expected to find %d chunks, got %d\n", cnt, len(filesHashes[i]))
					}

					if !bytes.Equal(data.Bytes(), bytesFiles[i]) {
						t.Fatalf("received chunk hash incompleted\n")
					}
				}

				data.Reset()
			}
		})
	}
}

func TestCheckInvalidTrie(t *testing.T) {
	testCases := []struct{
		name string
		reference string
		trieData map[string][]byte
		expectedError error
		unexpectedHashes []string
	}{
		{
			name: "invalid data",
			reference: "aa4a46bfbdff91c8db555edcfa4ba18371a083fdec67120db58d7ef177815ff0",
			trieData: map[string][]byte{
				"aa4a46bfbdff91c8db555edcfa4ba18371a083fdec67120db58d7ef177815ff0": {5,0,0,0,0,0,0,0,26,43,60,77,95}, // span: 5, data: 1a2b3c4d5f
			},
			expectedError: traversal.ErrInvalidTrie,
		},
		{
			name: "missing chunk",
			reference: "7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809",
			trieData: map[string][]byte{
				"7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809": { // root
					64,0,0,0,0,0,0,0,253,150,250,179,163,216,209,242,94,191,188,56,
					166,62,223,17,97,127,57,45,218,169,167,108,128,38,81,106,13,149,178,25,
					151,119,171,248,211,192,94,230,194,234,189,127,198,143,33,77,234,155,127,254,
					57,118,218,10,206,121,83,255,35,187,105,210,
				},
				"9777abf8d3c05ee6c2eabd7fc68f214dea9b7ffe3976da0ace7953ff23bb69d2": { // metadata
					118,0,0,0,0,0,0,0,123,34,109,105,109,101,116,121,112,101,34,58,
					34,116,101,120,116,47,112,108,97,105,110,59,32,99,104,97,114,115,101,116,
					61,117,116,102,45,56,34,44,34,102,105,108,101,110,97,109,101,34,58,34,
					102,100,57,54,102,97,98,51,97,51,100,56,100,49,102,50,53,101,98,102,
					98,99,51,56,97,54,51,101,100,102,49,49,54,49,55,102,51,57,50,100,
					100,97,97,57,97,55,54,99,56,48,50,54,53,49,54,97,48,100,57,53,
					98,50,49,57,34,125,
				},
				//"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219": { // file
				//	1,0,4,0,0,0,0,0,252,238,96,162,3,214,79,152,129,41,101,5,
				//	197,243,179,120,66,247,251,68,63,10,97,90,183,245,190,104,43,70,144,180,
				//	223,67,133,91,201,237,85,26,155,83,237,212,188,30,187,0,126,231,91,97,
				//	179,180,145,225,170,59,67,134,37,96,145,236,
				//},
				//"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec": { // chunk 1
				//	1,0,0,0,0,0,0,0,104,
				//},
				// "fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4": {} // chunk 2
			},
			expectedError: storage.ErrNotFound,
		},
		{
			name: "unused chunks",
			reference: "7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809",
			trieData: map[string][]byte{
				"7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809": { // root
					64,0,0,0,0,0,0,0,253,150,250,179,163,216,209,242,94,191,188,56,
					166,62,223,17,97,127,57,45,218,169,167,108,128,38,81,106,13,149,178,25,
					151,119,171,248,211,192,94,230,194,234,189,127,198,143,33,77,234,155,127,254,
					57,118,218,10,206,121,83,255,35,187,105,210,
				},
				"9777abf8d3c05ee6c2eabd7fc68f214dea9b7ffe3976da0ace7953ff23bb69d2": { // metadata
					118,0,0,0,0,0,0,0,123,34,109,105,109,101,116,121,112,101,34,58,
					34,116,101,120,116,47,112,108,97,105,110,59,32,99,104,97,114,115,101,116,
					61,117,116,102,45,56,34,44,34,102,105,108,101,110,97,109,101,34,58,34,
					102,100,57,54,102,97,98,51,97,51,100,56,100,49,102,50,53,101,98,102,
					98,99,51,56,97,54,51,101,100,102,49,49,54,49,55,102,51,57,50,100,
					100,97,97,57,97,55,54,99,56,48,50,54,53,49,54,97,48,100,57,53,
					98,50,49,57,34,125,
				},
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219": { // file
					1,0,4,0,0,0,0,0,252,238,96,162,3,214,79,152,129,41,101,5,
					197,243,179,120,66,247,251,68,63,10,97,90,183,245,190,104,43,70,144,180,
					223,67,133,91,201,237,85,26,155,83,237,212,188,30,187,0,126,231,91,97,
					179,180,145,225,170,59,67,134,37,96,145,236,
				},
				"9b645cc6aaae3b68a95904cc8e293545a5ed70c6aff4e9d983bf8dc0e28d3447": {5,0,0,0,0,0,0,0,209,9,26,188,222}, // wrong data
			},
			unexpectedHashes: []string{
				"9b645cc6aaae3b68a95904cc8e293545a5ed70c6aff4e9d983bf8dc0e28d3447",
			},
		},
	}

	for _, tc := range testCases {
		addrBytes, _ := hex.DecodeString(tc.reference)
		addr := boson.NewAddress(addrBytes)

		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mockStore := mock.NewStorer()

			traversalService := traversal.NewService(mockStore)
			_, err := traversalService.CheckTrieData(ctx, addr, tc.trieData)

			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected to handle error: %v\n\tgot error: %v", tc.expectedError, err)
			}

			if len(tc.unexpectedHashes) > 0 {
				for _, h := range tc.unexpectedHashes {
					addr, err := hex.DecodeString(h)
					if err != nil {
						t.Fatal(err)
					}
					_, err = mockStore.Get(ctx, storage.ModeGetRequest, boson.NewAddress(addr))
					if !errors.Is(err, storage.ErrNotFound) {
						t.Fatal(err)
					}
				}
			}
		})
	}
}