package traversal_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
	"math"
	"path"
	"sync"
	"testing"
	"time"
)

const fileContentType = "text/plain; charset=utf-8"

var (
	debug = false
	enableLargeTest = true
)

func verifyChunkHash(t *testing.T, ctx context.Context, store storage.Storer, rootHash boson.Address) {
	val, err := store.Get(ctx, storage.ModeGetRequest, rootHash)
	if err != nil {
		t.Fatal(err)
	}

	chunkData := val.Data()
	t.Logf("get val data size=%d\n", len(chunkData))
	if len(chunkData) > 16 && len(chunkData) <= boson.BigChunkSize+8 {
		trySpan := chunkData[:8]
		if size := binary.LittleEndian.Uint64(trySpan); size > uint64(len(chunkData[8:])) {
			for i := uint64(8); i < uint64(len(chunkData)-8); i += 32 {
				t.Logf("bmt root hash is %s\n", hex.EncodeToString(chunkData[i:i+32]))
			}
		} else {
			t.Logf("bmt root hash is %s, span = %d\n", rootHash.String(), size)
		}
	}
}

func uploadFile(t *testing.T, ctx context.Context, store storage.Storer, file []byte, filename string, encrypted bool) (boson.Address, string) {
	pipe := builder.NewPipelineBuilder(ctx, store, storage.ModePutUpload, encrypted)
	fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(file), int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}

	if debug {
		verifyChunkHash(t, ctx, store, fr)
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
	t.Logf("metadata hash=%s\n", mr)

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
	t.Logf("reference hash=%s\n", reference)

	return reference, filename
}

func uploadDirs(t *testing.T, ctx context.Context, store storage.Storer, manifestType string, files []file, encrypted bool) boson.Address {
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

	for _, f := range files {
		bytesData := generateSampleData(f.size)
		reference, filename := uploadFile(t, ctx, store, bytesData, f.name, encrypted)
		filePath := path.Join(f.dir, filename)
		err = dirManifest.Add(ctx, filePath, manifest.NewEntry(reference, nil))
		if err != nil {
			t.Fatal(err)
		}
	}

	manifestReference, err := dirManifest.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}

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
	t.Logf("manifest metadata hash=%s\n", mr)

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
	t.Logf("manifest reference hash=%s\n", reference)

	return reference
}

func TestGetFileTrieFromRemote(t *testing.T) {
	testCases := []struct {
		filesSize           int
		expectedHashesCount int
		expectedHashes      []string
		expectedFileCount   int
		expectedFileHashes  []string
	}{
		{
			filesSize:           boson.BigChunkSize,
			expectedHashesCount: 3,
			expectedHashes: []string{
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
				"4d63a3450eb5adf0ec9941c2997b0adca6d90a7e8dc1f16cc5638dd527308225",
				"d22aa075a95cea77c8ce7e5f8777cb9e0c620b77a1f7372df87f5e45d7c678d1",
			},
			expectedFileCount: 1,
			expectedFileHashes: []string{
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
			},
		},
		{
			filesSize:           boson.BigChunkSize + 1,
			expectedHashesCount: 5,
			expectedHashes: []string{
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219",
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
				"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec",
				"9777abf8d3c05ee6c2eabd7fc68f214dea9b7ffe3976da0ace7953ff23bb69d2",
				"7ccf77783acd807f7b3a50e4e73368181cc1886fc51d8556557515b9c4065809",
			},
			expectedFileCount: 2,
			expectedFileHashes: []string{
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
				"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec",
			},
		},
	}

	for _, tc := range testCases {
		chunkCount := int(math.Ceil(float64(tc.filesSize) / boson.BigChunkSize))
		testName := fmt.Sprintf("%d-chunk-%d-bytes", chunkCount, tc.filesSize)
		t.Run(testName, func(t *testing.T) {
			var (
				mockStoreA            *mock.MockStorer
				mockStoreB            *mock.MockStorer
				foundAddressForAMutex sync.Mutex
			)

			ctx := context.Background()
			bytesData := generateSampleData(tc.filesSize)

			mockStoreA = mock.NewStorer()

			reference, _ := uploadFile(t, ctx, mockStoreA, bytesData, "", false)
			traversalService := traversal.NewService(mockStoreA)

			foundAddressForACount := 0
			foundAddressForAMap := make(map[string]struct{})
			addressIterFn := func(addr boson.Address) error {
				foundAddressForAMutex.Lock()
				defer foundAddressForAMutex.Unlock()

				foundAddressForACount++
				foundAddressForAMap[addr.String()] = struct{}{}
				t.Log(addr.String())
				return nil
			}
			err := traversalService.TraverseFileAddresses(ctx, reference, addressIterFn)
			if err != nil {
				t.Fatal(err)
			}

			if foundAddressForACount != tc.expectedHashesCount {
				t.Fatalf("expected to find %d addresses, got %d", tc.expectedHashesCount, foundAddressForACount)
			}

			checkAddressFound := func(t *testing.T, foundAddresses map[string]struct{}, address string) {
				t.Helper()

				if _, ok := foundAddresses[address]; !ok {
					t.Fatalf("expected address %s not found", address)
				}
			}

			for _, createdAddress := range tc.expectedHashes {
				checkAddressFound(t, foundAddressForAMap, createdAddress)
			}

			mockStoreB = mock.NewStorer()

			trieData, err := traversalService.GetTrieData(ctx, reference)
			if err != nil {
				t.Fatal(err)
			}

			for existHash := range trieData {
				checkAddressFound(t, foundAddressForAMap, existHash)
			}

			traversalService = traversal.NewService(mockStoreB)
			chunkHashes, err := traversalService.CheckTrieData(ctx, reference, trieData)
			if err != nil {
				t.Fatal(err)
			}

			foundChunkMap := make(map[string]struct{})
			for _, file := range chunkHashes {
				for _, chunkHash := range file {
					foundChunkMap[hex.EncodeToString(chunkHash)] = struct{}{}
				}
			}

			if len(foundChunkMap) != tc.expectedFileCount {
				t.Fatalf("expected to find %d chunk, got %d", tc.expectedFileCount, len(foundChunkMap))
			}

			for _, remoteHash := range tc.expectedFileHashes {
				checkAddressFound(t, foundChunkMap, remoteHash)
			}
		})
	}
}

func TestGetLargeFileTrieFromRemote(t *testing.T) {
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
	largeBytesData := generateSampleData(boson.BigChunkSize * chunkCount)

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

func TestGetDirsTrieFromRemote(t *testing.T) {
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
				"6c0f20f4e792eb1be0fcba5693eb6878d6c17bb38ca05b7427f11c30975658d5",
				"06597037b942a720696ed5bd109a8d7dbd4f02a5ab01a2a8fb77e2b3b66d51ec",
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
					size: boson.BigChunkSize,
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
					size: boson.BigChunkSize,
					dir:       "",
					name:      "data/2.txt",
					reference: "5f3f1e36ff51200928ec698045282e8574cd0425ff5555e9c1f8c6253c4426ca",
					chunks: fileChunks{
						metadata: "b64e9d481a2c1e267f80fff87a67e58f50214a01396dc1729945bca87620cec6",
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
						},
					},
				},
			},
			manifestHashes: []string{
				"0b0859367e15bbe082291d7606e00f51c07a8f6568396d53540555e74459d5ed",
				"98abdef3d016e3baf9bfdf22f304972e4425f87e6a6827eb74e3665e2e2760c2",
				"b4aed5f5b94703de1fd1a7bd37ebbc33c52739b692ab96df13540b073183065e",
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
				"aa33cac9a6485bcc2dec0621dd9a6632656e614e3c3a15b167deec788a732ba3",
				"26e36da1582136cbba2d098bba5cdb029ff90895ca423ad720a73923f00db2c6",
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
					reference: "5962d00156551fd18fb7010ced14645a886f6f07af4c58e29ad4e48bd53359fa",
					chunks: fileChunks{
						metadata: "38a376fa1720f5814a197991b210f5b02f1b0e691467f09015d066068fb550ac",
						content: []string{
							"00051b571387a3e3b61d7a0d02ee5a509ecfc277f2841d57f10c0beff7b68736",
						},
					},
				},
				{
					size:      boson.ChunkSize,
					dir:       "",
					name:      "data/2.txt",
					reference: "963fc53eea045b86ad5f094ed76769c1cbc8b148de229288a3457a488cff43f2",
					chunks: fileChunks{
						metadata: "b64e9d481a2c1e267f80fff87a67e58f50214a01396dc1729945bca87620cec6",
						content: []string{
							"00051b571387a3e3b61d7a0d02ee5a509ecfc277f2841d57f10c0beff7b68736",
						},
					},
				},
			},
			manifestHashes: []string{
				"c5b7b18aba48b263fb4a0173416607d1688ee4ad822eebfc58967dbc2d86b57e", // metadata
				"16bb4b4137b2eeb903c94bba8e9ce292c9764281911d58f1768a5f76df873943", // root
				"bda7bbb317d6436f65df3c8ffa070437cd3d245b554054856ab9d54236e614ea",
				"92e6944828281cd235cf74f81f08dc8f55f83b1a6a1a264c7f3c851a86a2b22a",
				"c831ffb118f56e95e541827f20189412ac189a5fdea18b3edaca436de24fef42",
				"20a2dd57660def162b5fbc39ece34c0874376763c1a6669052a8a120f33f0bfa",
				"185f1d1f53c07670dffa7adc4836156fe86c8b2e970c2614923a43e1e51e9f78",
			},
			expectedHashesCount: 16,
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
			var (
				mockStoreA *mock.MockStorer
				mockStoreB *mock.MockStorer
				foundAddressForAMutex sync.Mutex
			)

			ctx := context.Background()
			mockStoreA = mock.NewStorer()

			manifestReference := uploadDirs(t, ctx, mockStoreA, tc.manifestType, tc.files, false)
			traversalService := traversal.NewService(mockStoreA)

			tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			foundAddressForACount := 0
			foundAddressForAMap := make(map[string]struct{})
			addressIterFn := func(addr boson.Address) error {
				foundAddressForAMutex.Lock()
				defer foundAddressForAMutex.Unlock()

				foundAddressForACount++
				foundAddressForAMap[addr.String()] = struct{}{}
				t.Log(addr.String())
				return nil
			}
			err := traversalService.TraverseManifestAddresses(tCtx, manifestReference, addressIterFn)
			if err != nil {
				t.Fatal(err)
			}

			if foundAddressForACount != tc.expectedHashesCount {
				t.Fatalf("expected to find %d addresses, got %d", tc.expectedHashesCount, foundAddressForACount)
			}

			checkAddressFound := func(t *testing.T, foundAddresses map[string]struct{}, address string) {
				t.Helper()

				if _, ok := foundAddresses[address]; !ok {
					t.Fatalf("expected address %s not found", address)
				}
			}

			expectedHashes := make([]string, 0)
			for _, f := range tc.files {
				expectedHashes = append(expectedHashes, f.reference, f.chunks.metadata)
				expectedHashes = append(expectedHashes, f.chunks.content...)
			}
			expectedHashes = append(expectedHashes, tc.manifestHashes...)

			for _, expectedHash := range expectedHashes {
				checkAddressFound(t, foundAddressForAMap, expectedHash)
			}

			mockStoreB = mock.NewStorer()

			trieData, err := traversalService.GetTrieData(ctx, manifestReference)
			if err != nil {
				t.Fatal(err)
			}

			for existHash := range trieData {
				checkAddressFound(t, foundAddressForAMap, existHash)
			}

			traversalService = traversal.NewService(mockStoreB)
			chunkHashes, err := traversalService.CheckTrieData(ctx, manifestReference, trieData)
			if err != nil {
				t.Fatal(err)
			}

			existsChunkHashes := make(map[string]struct{}, 0)
			for _, file := range chunkHashes {
				for _, hash := range file {
					existsChunkHashes[hex.EncodeToString(hash)] = struct{}{}
				}
			}

			for _, file := range tc.files {
				for _, hash := range file.chunks.content {
					checkAddressFound(t, existsChunkHashes, hash)
				}
			}
		})
	}
}