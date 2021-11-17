package traversal_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline"
	"github.com/gauss-project/aurorafs/pkg/file/pipeline/builder"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

var (
	dataCorpus       = []byte("hello test world") // fixed, 16 bytes
	defaultMediaType = "aurora-manifest-mantaray"
	enableLargeTest  = false
)

type chunkReader struct {
	sample []byte
	limit  int
	count  int
	index  int
	total  uint64
	offset uint64
}

func newChunkReader(data []byte, perSize int, length uint64) *chunkReader {
	r := &chunkReader{
		sample: data,
		total:  length,
	}

	uPerSize := uint64(perSize)

	if length <= uPerSize {
		r.limit = int(length)
		r.count = 1
	} else {
		r.limit = perSize
		r.count = int(length / uPerSize)
		if length%uPerSize != 0 {
			r.count++
		}
	}

	return r
}

// Read implements the io.Reader interface.
func (r *chunkReader) Read(b []byte) (n int, err error) {
	if r.offset >= r.total {
		return 0, io.EOF
	}

	for n < len(b) {
		var i int
		if r.total-r.offset <= uint64(len(r.sample)) {
			i = copy(b[n:], r.sample[r.index:r.index+int(r.total-r.offset)])
		} else {
			i = copy(b[n:], r.sample[r.index:])
		}
		r.index = (r.index + i) % len(r.sample)
		n += i
		r.offset += uint64(i)
		if r.offset == r.total {
			break
		}
	}

	return n, nil
}

func generateSample(size int) (b []byte) {
	buf := make([]byte, size)
	for n := 0; n < size; {
		n += copy(buf[n:], dataCorpus)
	}
	return buf
}

// newAddressIterator is a convenient constructor for creating addressIterator.
func newAddressIterator(ignoreDuplicates bool) *addressIterator {
	return &addressIterator{
		ignoreDuplicates: ignoreDuplicates,
	}
}

// addressIterator is a simple collector of statistics
// targeting swarm.AddressIterFunc execution.
type addressIterator struct {
	cnt  int32
	num  int32
	seen sync.Map
	// Settings.
	ignoreDuplicates bool
}

// Next matches the signature of swarm.AddressIterFunc needed in
// Traverser.Traverse method and collects statistics about it's execution.
func (i *addressIterator) Next(addr boson.Address) error {
	atomic.AddInt32(&i.cnt, 1)
	_, loaded := i.seen.LoadOrStore(addr.String(), true)
	if !loaded {
		atomic.AddInt32(&i.num, 1)
	} else if !i.ignoreDuplicates {
		return fmt.Errorf("duplicit address: %q", addr.String())
	}
	return nil
}

func TestTraversalBytes(t *testing.T) {
	testCases := []struct {
		dataSize              int
		wantHashCount         int
		wantHashes            []string
		ignoreDuplicateHashes bool
	}{
		{
			dataSize:      len(dataCorpus),
			wantHashCount: 1,
			wantHashes: []string{
				"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
			},
		},
		{
			dataSize:      boson.ChunkSize,
			wantHashCount: 1,
			wantHashes: []string{
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
			},
		},
		{
			dataSize:      boson.ChunkSize + 1,
			wantHashCount: 3,
			wantHashes: []string{
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219", // bytes (joiner)
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
				"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec", // bytes (1)
			},
		},
		{
			dataSize:      boson.ChunkSize * 8192,
			wantHashCount: 8193,
			wantHashes: []string{
				"e9d65e92d1eb53ea6023af3843c27538f6a648a083fdbdd7df9e57796de24672", // bytes (joiner)
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
			},
			ignoreDuplicateHashes: true,
		},
		{
			dataSize:      boson.ChunkSize * 8193,
			wantHashCount: 8195,
			wantHashes: []string{
				"dfb776a36e9a3f692f45e3c4e35ec71eb2d4e4bacfa3946c59f38f1d5b9e4bed", // root (joiner, chunk)
				"e9d65e92d1eb53ea6023af3843c27538f6a648a083fdbdd7df9e57796de24672", // bytes (joiner)
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
			},
			ignoreDuplicateHashes: true,
		},
		{
			dataSize:      boson.ChunkSize*8193 - 1,
			wantHashCount: 8195,
			wantHashes: []string{
				"d87560048ac023f750ace0cd59882f96e128cde527d27f8b78e2cf24ae59da04", // root (joiner, chunk)
				"e9d65e92d1eb53ea6023af3843c27538f6a648a083fdbdd7df9e57796de24672", // bytes (joiner)
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
				"420c51c38682c1df5a027a1e89f3280607bca9337263b11f09e6a97dbe4087fc", // bytes (chunkSize - 1)
			},
			ignoreDuplicateHashes: true,
		},
		{
			dataSize:      boson.ChunkSize*8193 + 1,
			wantHashCount: 8197,
			wantHashes: []string{
				"581e536558f70aaf6229e811877d8b54e600bca3dcef47355e5af8bb9c9db663", // root (joiner, chunk)
				"e9d65e92d1eb53ea6023af3843c27538f6a648a083fdbdd7df9e57796de24672", // bytes (joiner [boson.ChunkSize * 8192])
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
				"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec", // bytes (1)
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219", // bytes (joiner - [boson.ChunkSize, 1])
			},
			ignoreDuplicateHashes: true,
		},
	}

	for _, tc := range testCases {
		chunkCount := int(math.Ceil(float64(tc.dataSize) / boson.ChunkSize))
		t.Run(fmt.Sprintf("%d-chunk-%d-bytes", chunkCount, tc.dataSize), func(t *testing.T) {
			var (
				data       = newChunkReader(dataCorpus, boson.ChunkSize, uint64(tc.dataSize))
				iter       = newAddressIterator(tc.ignoreDuplicateHashes)
				storerMock = mock.NewStorer()
			)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			pipe := builder.NewPipelineBuilder(ctx, storerMock, storage.ModePutUpload, false)
			address, err := builder.FeedPipeline(ctx, pipe, data)
			if err != nil {
				t.Fatal(err)
			}

			err = traversal.New(storerMock).Traverse(ctx, address, iter.Next)
			if err != nil {
				t.Fatal(err)
			}

			haveCnt, wantCnt := tc.wantHashCount, int(iter.cnt)
			if !tc.ignoreDuplicateHashes {
				haveCnt, wantCnt = int(iter.num), len(tc.wantHashes)
			}
			if haveCnt != wantCnt {
				t.Fatalf("hash count mismatch: have %d; want %d", haveCnt, wantCnt)
			}

			for _, hash := range tc.wantHashes {
				_, loaded := iter.seen.Load(hash)
				if !loaded {
					t.Fatalf("hash check: want %q; have none", hash)
				}
			}
		})
	}

}

func TestTraversalFiles(t *testing.T) {
	testCases := []struct {
		filesSize             int
		contentType           string
		filename              string
		wantHashCount         int
		wantHashes            []string
		ignoreDuplicateHashes bool
	}{
		{
			filesSize:     len(dataCorpus),
			contentType:   "text/plain; charset=utf-8",
			filename:      "simple.txt",
			wantHashCount: 4,
			wantHashes: []string{
				"857ed783bf9b74e57e3a15e2e701653dbf9b55efcdb658aaedb5f693af1cf4a9", // root manifest
				"8a25e1a4d4cb78bfe85d0fd6a212edccef318ae0b7fcb3ff27769d40dc474eb0", // mainifest root metadata
				"6b84ee3e769244b7b2febaafe0351ddd9d847965d8885539eb0ed4de02c67ae9", // manifest file entry
				"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279", // bytes
			},
		},
		{
			filesSize:     boson.ChunkSize,
			contentType:   "text/plain; charset=utf-8",
			wantHashCount: 6,
			wantHashes: []string{
				"4bc83bba4ee0141000e96dba1a8909877e58f672e8e0df1c3d0fa7de9897fabb", // root manifest
				"8a25e1a4d4cb78bfe85d0fd6a212edccef318ae0b7fcb3ff27769d40dc474eb0", // manifest root metadata
				"071257f2f13721bdaf62e29c6b5dcff21db85fb040f6305e50ee267f63e1083a", // manifest file entry (Edge)
				"86ab4d031222cbb847baba597f2fc009395622e5ef6b9bbc24e4af52c3e213da", // manifest file entry (Edge)
				"78f35301b4d227d06ac86569921935a39b4f16e33248e1512f8292252c24db35", // manifest file entry (Value)
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes
			},
		},
		{
			filesSize:     boson.ChunkSize + 1,
			contentType:   "text/plain; charset=utf-8",
			filename:      "simple.txt",
			wantHashCount: 6,
			wantHashes: []string{
				"e3e46b84167a2991ef7a3c0174ad3455fc427e98d3c00c2dfe04c0c50d142d2a", // manifest root
				"8a25e1a4d4cb78bfe85d0fd6a212edccef318ae0b7fcb3ff27769d40dc474eb0", // manifest root metadata
				"11249b9344c92af843ee2092f7b11527c6058e21319a4e8371d713aa72c7bae5", // manifest file entry
				"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219", // manifest file entry
				"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
				"df43855bc9ed551a9b53edd4bc1ebb007ee75b61b3b491e1aa3b4386256091ec", // bytes (1)
			},
		},
	}

	for _, tc := range testCases {
		chunkCount := int(math.Ceil(float64(tc.filesSize) / boson.ChunkSize))
		t.Run(fmt.Sprintf("%d-chunk-%d-bytes", chunkCount, tc.filesSize), func(t *testing.T) {
			var (
				data       = generateSample(tc.filesSize)
				iter       = newAddressIterator(tc.ignoreDuplicateHashes)
				storerMock = mock.NewStorer()
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			pipe := builder.NewPipelineBuilder(ctx, storerMock, storage.ModePutUpload, false)
			fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}

			ls := loadsave.New(storerMock, pipelineFactory(storerMock, storage.ModePutRequest, false))
			fManifest, err := manifest.NewDefaultManifest(ls, false)
			if err != nil {
				t.Fatal(err)
			}
			filename := tc.filename
			if filename == "" {
				filename = fr.String()
			}

			rootMtdt := map[string]string{
				manifest.WebsiteIndexDocumentSuffixKey: filename,
			}
			err = fManifest.Add(ctx, "/", manifest.NewEntry(boson.ZeroAddress, rootMtdt))
			if err != nil {
				t.Fatal(err)
			}

			fileMtdt := map[string]string{
				manifest.EntryMetadataFilenameKey:    filename,
				manifest.EntryMetadataContentTypeKey: tc.contentType,
			}
			err = fManifest.Add(ctx, filename, manifest.NewEntry(fr, fileMtdt))
			if err != nil {
				t.Fatal(err)
			}

			address, err := fManifest.Store(ctx)
			if err != nil {
				t.Fatal(err)
			}

			err = traversal.New(storerMock).Traverse(ctx, address, iter.Next)
			if err != nil {
				t.Fatal(err)
			}

			haveCnt, wantCnt := tc.wantHashCount, int(iter.cnt)
			if !tc.ignoreDuplicateHashes {
				haveCnt, wantCnt = int(iter.num), len(tc.wantHashes)
			}
			if haveCnt != wantCnt {
				t.Fatalf("hash count mismatch: have %d; want %d", haveCnt, wantCnt)
			}

			for _, hash := range tc.wantHashes {
				_, loaded := iter.seen.Load(hash)
				if !loaded {
					t.Fatalf("hash check: want %q; have none", hash)
				}
			}
		})
	}

}

type file struct {
	size   int
	dir    string
	name   string
	chunks fileChunks
}

type fileChunks struct {
	content []string
}

func TestTraversalManifest(t *testing.T) {
	testCases := []struct {
		files                 []file
		manifestHashes        []string
		wantHashCount         int
		ignoreDuplicateHashes bool
	}{
		{
			files: []file{
				{
					size: len(dataCorpus),
					dir:  "",
					name: "hello.txt",
					chunks: fileChunks{
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
			},
			manifestHashes: []string{
				// NOTE: references will be fixed, due to custom obfuscation key function
				"663b1756ca8ceaf6ba0e01017ade01e6d46a9e9a62e5b0236f806919561f9132", // root
				"6b84ee3e769244b7b2febaafe0351ddd9d847965d8885539eb0ed4de02c67ae9", // metadata
			},
			wantHashCount: 3,
		},
		{
			files: []file{
				{
					size: len(dataCorpus),
					dir:  "",
					name: "hello.txt",
					chunks: fileChunks{
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
				{
					size: boson.ChunkSize,
					dir:  "",
					name: "data/1.txt",
					chunks: fileChunks{
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
						},
					},
				},
				{
					size: boson.ChunkSize,
					dir:  "",
					name: "data/2.txt",
					chunks: fileChunks{
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4", // bytes (chunkSize)
						},
					},
				},
			},
			manifestHashes: []string{
				// NOTE: references will be fixed, due to custom obfuscation key function
				"ebc4be67fdfd4950eb73ae7540715ec7aad09d5928210bec2a83ef074e354b21", // root
				"6b84ee3e769244b7b2febaafe0351ddd9d847965d8885539eb0ed4de02c67ae9", // manifest entry
				"99ebc4d4056b0f79d47bc2872e60de05d4ead293c2baac1765ac727ac37234e9", // manifest entry (Edge PathSeparator)
				"78f35301b4d227d06ac86569921935a39b4f16e33248e1512f8292252c24db35", // manifest file entry (1.txt)
				"78f35301b4d227d06ac86569921935a39b4f16e33248e1512f8292252c24db35", // manifest file entry (2.txt)
			},
			wantHashCount:         8,
			ignoreDuplicateHashes: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%d-files-%d-chunks", defaultMediaType, len(tc.files), tc.wantHashCount), func(t *testing.T) {
			var (
				storerMock = mock.NewStorer()
				iter       = newAddressIterator(tc.ignoreDuplicateHashes)
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var wantHashes []string
			for _, f := range tc.files {
				wantHashes = append(wantHashes, f.chunks.content...)
			}
			wantHashes = append(wantHashes, tc.manifestHashes...)

			ls := loadsave.New(storerMock, pipelineFactory(storerMock, storage.ModePutRequest, false))
			dirManifest, err := manifest.NewMantarayManifest(ls, false)
			if err != nil {
				t.Fatal(err)
			}

			for _, f := range tc.files {
				data := generateSample(f.size)

				pipe := builder.NewPipelineBuilder(ctx, storerMock, storage.ModePutUpload, false)
				fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}

				fileName := f.name
				if fileName == "" {
					fileName = fr.String()
				}
				filePath := path.Join(f.dir, fileName)

				err = dirManifest.Add(ctx, filePath, manifest.NewEntry(fr, nil))
				if err != nil {
					t.Fatal(err)
				}
			}
			address, err := dirManifest.Store(ctx)
			if err != nil {
				t.Fatal(err)
			}

			err = traversal.New(storerMock).Traverse(ctx, address, iter.Next)
			if err != nil {
				t.Fatal(err)
			}

			haveCnt, wantCnt := tc.wantHashCount, int(iter.cnt)
			if !tc.ignoreDuplicateHashes {
				haveCnt, wantCnt = int(iter.num), len(wantHashes)
			}
			if haveCnt != wantCnt {
				t.Fatalf("hash count mismatch: have %d; want %d", haveCnt, wantCnt)
			}

			for _, hash := range wantHashes {
				_, loaded := iter.seen.Load(hash)
				if !loaded {
					t.Fatalf("hash check: want %q; have none", hash)
				}
			}
		})
	}

}

func TestGetPyramid(t *testing.T) {
	testCases := []struct {
		files          []file
		manifestHashes []string
		wantHashCount  int
	}{
		{
			files: []file{
				{
					size: len(dataCorpus),
					dir:  "",
					name: "hello.txt",
					chunks: fileChunks{
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
			},
			manifestHashes: []string{
				// NOTE: references will be fixed, due to custom obfuscation key function
				"663b1756ca8ceaf6ba0e01017ade01e6d46a9e9a62e5b0236f806919561f9132", // root
				"6b84ee3e769244b7b2febaafe0351ddd9d847965d8885539eb0ed4de02c67ae9", // manifest entry
			},
			wantHashCount: 3,
		},
		{
			files: []file{
				{
					size: len(dataCorpus),
					dir:  "",
					name: "hello.txt",
					chunks: fileChunks{
						content: []string{
							"a71ef6088e53d326c7c8bfc3749ccda74d1c455d6c65538b6a36eb47cfec6279",
						},
					},
				},
				{
					size: boson.ChunkSize,
					dir:  "",
					name: "data/1.txt",
					chunks: fileChunks{
						content: []string{
							"fcee60a203d64f9881296505c5f3b37842f7fb443f0a615ab7f5be682b4690b4",
						},
					},
				},
				{
					size: boson.ChunkSize + 1,
					dir:  "",
					name: "data/2.txt",
					chunks: fileChunks{
						content: []string{
							"fd96fab3a3d8d1f25ebfbc38a63edf11617f392ddaa9a76c8026516a0d95b219",
						},
					},
				},
			},
			manifestHashes: []string{
				// NOTE: references will be fixed, due to custom obfuscation key function
				"737c4ad321d1b2170fe3ea0b091db5382738b97132558cfac407cc12f03e45e8", // root
				"6b84ee3e769244b7b2febaafe0351ddd9d847965d8885539eb0ed4de02c67ae9", // manifest entry
				"98efbf4434473a02b366205475721e7a3c68ee99a2dffdadd3029debdbd7cee2", // manifest entry (Edge PathSeparator)
				"78f35301b4d227d06ac86569921935a39b4f16e33248e1512f8292252c24db35", // manifest file entry (1.txt)
				"11249b9344c92af843ee2092f7b11527c6058e21319a4e8371d713aa72c7bae5", // manifest file entry (2.txt)
			},
			wantHashCount: 8,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-files-%d-chunks", len(tc.files), tc.wantHashCount), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			storerMock := mock.NewStorer()

			ls := loadsave.New(storerMock, pipelineFactory(storerMock, storage.ModePutRequest, false))
			dirManifest, err := manifest.NewMantarayManifest(ls, false)
			if err != nil {
				t.Fatal(err)
			}

			for _, f := range tc.files {
				data := generateSample(f.size)

				pipe := builder.NewPipelineBuilder(ctx, storerMock, storage.ModePutUpload, false)
				fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}

				fileName := f.name
				if fileName == "" {
					fileName = fr.String()
				}
				filePath := path.Join(f.dir, fileName)

				err = dirManifest.Add(ctx, filePath, manifest.NewEntry(fr, nil))
				if err != nil {
					t.Fatal(err)
				}
			}
			address, err := dirManifest.Store(ctx)
			if err != nil {
				t.Fatal(err)
			}

			traversalService := traversal.New(storerMock)

			expectedHashes := make([]string, 0)
			for _, f := range tc.files {
				expectedHashes = append(expectedHashes, f.chunks.content...)
			}
			expectedHashes = append(expectedHashes, tc.manifestHashes...)

			pyramid, err := traversalService.GetPyramid(ctx, address)
			if err != nil {
				t.Fatal(err)
			}

			if len(pyramid) != tc.wantHashCount {
				t.Fatalf("expected to find %d addresses, got %d", tc.wantHashCount, len(pyramid))
			}

			foundHashes := make(map[string]struct{})
			for hash := range pyramid {
				foundHashes[hash] = struct{}{}
			}

			checkAddressFound := func(t *testing.T, hash string) {
				t.Helper()

				if _, ok := foundHashes[hash]; !ok {
					t.Fatalf("expected address %s not found", hash)
				}
			}

			for _, expectedHash := range expectedHashes {
				checkAddressFound(t, expectedHash)
			}
		})
	}
}

func TestGetChunkHashes(t *testing.T) {
	testCases := []struct {
		files []file
	}{
		{
			files: []file{
				{
					size: len(dataCorpus),
					dir:  "",
					name: "hello.txt",
				},
				{
					size: boson.ChunkSize + 1,
					dir:  "",
					name: "data/1.txt",
				},
			},
		},
		{
			files: []file{
				{
					size: boson.ChunkSize * 2,
					dir:  "",
					name: "hello.txt",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-files", len(tc.files)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			storerMockA := mock.NewStorer()
			storerMockB := mock.NewStorer()

			sort.Slice(tc.files, func(i, j int) bool {
				return tc.files[i].name < tc.files[j].name
			})

			ls := loadsave.New(storerMockA, pipelineFactory(storerMockA, storage.ModePutRequest, false))
			dirManifest, err := manifest.NewMantarayManifest(ls, false)
			if err != nil {
				t.Fatal(err)
			}

			files := make([][]byte, len(tc.files))

			for i, f := range tc.files {
				data := generateSample(f.size)

				pipe := builder.NewPipelineBuilder(ctx, storerMockA, storage.ModePutUpload, false)
				fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}

				fileName := f.name
				if fileName == "" {
					fileName = fr.String()
				}
				filePath := path.Join(f.dir, fileName)

				err = dirManifest.Add(ctx, filePath, manifest.NewEntry(fr, nil))
				if err != nil {
					t.Fatal(err)
				}

				files[i] = data
			}
			address, err := dirManifest.Store(ctx)
			if err != nil {
				t.Fatal(err)
			}

			traversalService := traversal.New(storerMockA)

			pyramid, err := traversalService.GetPyramid(ctx, address)
			if err != nil {
				t.Fatal(err)
			}

			traversalService = traversal.New(storerMockB)
			filesHashes, filesChecked, err := traversalService.GetChunkHashes(ctx, address, pyramid)
			if err != nil {
				t.Fatal(err)
			}

			sort.Slice(filesHashes, func(i, j int) bool {
				return len(filesHashes[i]) > len(filesHashes[j])
			})

			data := bytes.NewBuffer([]byte{})
			for i, f := range filesHashes {
				for j, h := range f {
					ch, err := storerMockA.Get(ctx, storage.ModeGetRequest, boson.NewAddress(h))
					if err != nil {
						t.Fatalf("%d chunks: %v\n", j, err)
					}
					data.Write(ch.Data()[8:])
				}

				if len(f) > 0 {
					cnt := tc.files[i].size / boson.ChunkSize
					if tc.files[i].size%boson.ChunkSize != 0 {
						cnt++
					}
					if len(filesHashes[i]) != cnt {
						t.Fatalf("expected to find %d chunks, got %d\n", cnt, len(filesHashes[i]))
					}
					if len(filesHashes[i]) == 1 {
						found := false
						for _, checkedHash := range filesChecked {
							if bytes.Equal(checkedHash, filesHashes[i][0]) {
								found = true
								break
							}
						}
						if !found {
							t.Fatalf("when file size smaller than default chunk size, it should be shown at checked hashes")
						}
					}

					if !bytes.Equal(data.Bytes(), files[i]) {
						t.Fatalf("received chunk hash incompleted\n")
					}
				}

				data.Reset()
			}
		})
	}
}

func TestGetChunkHashesForLarge(t *testing.T) {
	if !enableLargeTest {
		t.Skipf("disable large file test\n")
	}

	ctx := context.Background()

	var (
		storerMockA      *mock.MockStorer
		storerMockB      *mock.MockStorer
		foundHashesMutex sync.Mutex
	)

	storerMockA = mock.NewStorer()

	chunkCount := 8192 + 1
	expectedFileHashCount := 3
	expectedChunkHashCount := chunkCount

	ls := loadsave.New(storerMockA, pipelineFactory(storerMockA, storage.ModePutRequest, false))
	dirManifest, err := manifest.NewMantarayManifest(ls, false)
	if err != nil {
		t.Fatal(err)
	}

	largeData := generateSample(boson.ChunkSize * chunkCount)

	pipe := builder.NewPipelineBuilder(ctx, storerMockA, storage.ModePutUpload, false)
	fr, err := builder.FeedPipeline(ctx, pipe, bytes.NewReader(largeData))
	if err != nil {
		t.Fatal(err)
	}

	err = dirManifest.Add(ctx, "largefile", manifest.NewEntry(fr, nil))
	if err != nil {
		t.Fatal(err)
	}

	address, err := dirManifest.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}

	traversalService := traversal.New(storerMockA)

	tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	foundHashesCount := 0

	if err = traversalService.Traverse(tCtx, address, func(_ boson.Address) error {
		foundHashesMutex.Lock()
		defer foundHashesMutex.Unlock()

		foundHashesCount++
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if foundHashesCount != expectedChunkHashCount+expectedFileHashCount+1 {
		t.Fatalf("expected to find %d chunk hash, got %d\n", expectedChunkHashCount+expectedFileHashCount+1, foundHashesCount)
	}

	storerMockB = mock.NewStorer()

	pyramid, err := traversalService.GetPyramid(ctx, address)
	if err != nil {
		t.Fatal(err)
	}

	// one intermediate full level
	if len(pyramid) != expectedFileHashCount+1 {
		t.Fatalf("expected to find %d file hash(root/file/metadata), got %d\n", expectedFileHashCount+1, len(pyramid))
	}

	traversalService = traversal.New(storerMockB)
	chunkHashes, pieces, err := traversalService.GetChunkHashes(ctx, address, pyramid)
	if err != nil {
		t.Fatal(err)
	}

	if len(pieces) > 0 {
		t.Fatalf("smaller than default chunk size MUST not display in here")
	}

	// only a file
	file := chunkHashes[0]
	if len(file) != expectedChunkHashCount {
		t.Fatalf("expected to find %d chunk hash, got %d\n", expectedChunkHashCount, len(file))
	}
}

func TestGetChunkHashesForInvalidPyramid(t *testing.T) {
	testCases := []struct {
		name             string
		reference        string
		pyramid          map[string][]byte
		expectedError    error
		unexpectedHashes []string
	}{
		{
			name:      "invalid pyramid",
			reference: "aa4a46bfbdff91c8db555edcfa4ba18371a083fdec67120db58d7ef177815ff0",
			pyramid: map[string][]byte{
				"aa4a46bfbdff91c8db555edcfa4ba18371a083fdec67120db58d7ef177815ff0": {5, 0, 0, 0, 0, 0, 0, 0, 26, 43, 60, 77, 95}, // span: 5, data: 1a2b3c4d5f
			},
			expectedError: traversal.ErrInvalidPyramid,
		},
		{
			name:      "missing chunk",
			reference: "5101d047d6a5d7252215c33f0d571824063386cb116fada8fc5d5bace4ac0b35",
			pyramid: map[string][]byte{
				"5101d047d6a5d7252215c33f0d571824063386cb116fada8fc5d5bace4ac0b35": { // root
					192, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 87, 104, 179, 182, 167, 219, 86, 210, 29, 26, 191, 244, 13, 65, 206, 191, 200, 52, 72, 254, 216, 215,
					233, 176, 110, 192, 211, 176, 115, 242, 143, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 2, 9, 104, 101, 108, 108, 111, 46, 116, 120, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 219, 16, 217, 77, 174, 27, 149, 180, 103, 247, 90, 73, 236, 93, 199, 197, 122, 229, 198, 67, 36,
					178, 131, 164, 137, 63, 108, 130, 244, 197, 252, 226,
				},
				"db10d94dae1b95b467f75a49ec5dc7c57ae5c64324b283a4893f6c82f4c5fce2": { // metadata
					128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 87, 104, 179, 182, 167, 219, 86, 210, 29, 26, 191, 244, 13, 65, 206, 191, 200, 52, 72, 254, 216,
					215, 233, 176, 110, 192, 211, 176, 115, 242, 143, 32, 54, 196, 100, 166, 177, 130, 159, 104, 116,
					14, 204, 129, 137, 134, 214, 26, 227, 200, 7, 131, 238, 30, 222, 96, 190, 78, 17, 40, 171, 15, 20,
					26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				},
				// "36c464a6b1829f68740ecc818986d61ae3c80783ee1ede60be4e1128ab0f141a": { // manifest entry
				// 	64,0,0,0,0,0,0,0,104,101,108,108,111,32,116,101,115,116,32,119,111,114,108,
				// 	100,104,101,108,108,111,32,116,101,115,116,32,119,111,114,108,100,104,101,
				// 	108,108,111,32,116,101,115,116,32,119,111,114,108,100,104,101,108,108,111,
				// 	32,116,101,115,116,32,119,111,114,108,100,
				// },
			},
			expectedError: storage.ErrNotFound,
		},
		{
			name:      "unused chunks",
			reference: "5101d047d6a5d7252215c33f0d571824063386cb116fada8fc5d5bace4ac0b35",
			pyramid: map[string][]byte{
				"5101d047d6a5d7252215c33f0d571824063386cb116fada8fc5d5bace4ac0b35": { // root
					192, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 87, 104, 179, 182, 167, 219, 86, 210, 29, 26, 191, 244, 13, 65, 206, 191, 200, 52, 72, 254, 216, 215,
					233, 176, 110, 192, 211, 176, 115, 242, 143, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 2, 9, 104, 101, 108, 108, 111, 46, 116, 120, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 219, 16, 217, 77, 174, 27, 149, 180, 103, 247, 90, 73, 236, 93, 199, 197, 122, 229, 198, 67, 36,
					178, 131, 164, 137, 63, 108, 130, 244, 197, 252, 226,
				},
				"db10d94dae1b95b467f75a49ec5dc7c57ae5c64324b283a4893f6c82f4c5fce2": { // metadata
					128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 87, 104, 179, 182, 167, 219, 86, 210, 29, 26, 191, 244, 13, 65, 206, 191, 200, 52, 72, 254, 216,
					215, 233, 176, 110, 192, 211, 176, 115, 242, 143, 32, 54, 196, 100, 166, 177, 130, 159, 104, 116,
					14, 204, 129, 137, 134, 214, 26, 227, 200, 7, 131, 238, 30, 222, 96, 190, 78, 17, 40, 171, 15, 20,
					26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				},
				"36c464a6b1829f68740ecc818986d61ae3c80783ee1ede60be4e1128ab0f141a": { // manifest entry
					64, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 116, 101, 115, 116, 32, 119, 111, 114, 108,
					100, 104, 101, 108, 108, 111, 32, 116, 101, 115, 116, 32, 119, 111, 114, 108, 100, 104, 101,
					108, 108, 111, 32, 116, 101, 115, 116, 32, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111,
					32, 116, 101, 115, 116, 32, 119, 111, 114, 108, 100,
				},
				"4beb27ee12758751f44db53a88f7928819c49d6cda980305561db595534eb904": { // wrong data
					32, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 116, 101, 115, 116, 32, 119, 111, 114, 108, 100, 104,
					101, 108, 108, 111, 32, 116, 101, 115, 116, 32, 119, 111, 114, 108, 100,
				},
			},
			unexpectedHashes: []string{
				"4beb27ee12758751f44db53a88f7928819c49d6cda980305561db595534eb904",
			},
		},
	}

	for _, tc := range testCases {
		addrBytes, _ := hex.DecodeString(tc.reference)
		addr := boson.NewAddress(addrBytes)

		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mockStore := mock.NewStorer()

			traversalService := traversal.New(mockStore)
			_, _, err := traversalService.GetChunkHashes(ctx, addr, tc.pyramid)

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

func pipelineFactory(s storage.Putter, mode storage.ModePut, encrypt bool) func() pipeline.Interface {
	return func() pipeline.Interface {
		return builder.NewPipelineBuilder(context.Background(), s, mode, encrypt)
	}
}
