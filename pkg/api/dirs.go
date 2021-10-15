package api

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	contentTypeHeader = "Content-Type"
	contentTypeTar    = "application/x-tar"
)

const (
	manifestRootPath                      = "/"
	manifestWebsiteIndexDocumentSuffixKey = "website-index-document"
	manifestWebsiteErrorDocumentPathKey   = "website-error-document"
)

// dirUploadHandler uploads a directory supplied as a tar in an HTTP request
func (s *server) dirUploadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	err := validateRequest(r)
	if err != nil {
		logger.Errorf("dir upload, validate request")
		logger.Debugf("dir upload, validate request err: %v", err)
		jsonhttp.BadRequest(w, "could not validate request")
		return
	}

	// Add the tag to the context
	ctx := r.Context()
	p := requestPipelineFn(s.storer, r)
	encrypt := requestEncrypt(r)
	factory := requestPipelineFactory(ctx, s.storer, r)
	l := loadsave.New(s.storer, factory)
	reference, err := storeDir(ctx, encrypt, r.Body, s.logger, p, l, r.Header.Get(BosonIndexDocumentHeader), r.Header.Get(BosonErrorDocumentHeader))
	if err != nil {
		logger.Debugf("dir upload: store dir err: %v", err)
		logger.Errorf("dir upload: store dir")
		jsonhttp.InternalServerError(w, "could not store dir")
		return
	}

	a, err := s.traversal.GetTrieData(ctx, reference)
	if err != nil {
		logger.Errorf("dir upload: get trie data err: %v", err)
		jsonhttp.InternalServerError(w, "could not get trie data")
		return
	}
	dataChunks, _ := s.traversal.CheckTrieData(ctx, reference, a)
	if err != nil {
		logger.Errorf("dir upload: check trie data err: %v", err)
		jsonhttp.InternalServerError(w, "check trie data error")
		return
	}
	for _, li := range dataChunks {
		for _, b := range li {
			s.chunkInfo.OnChunkTransferred(boson.NewAddress(b), reference, s.overlay)
		}
	}

	jsonhttp.OK(w, fileUploadResponse{
		Reference: reference,
	})
}

// validateRequest validates an HTTP request for a directory to be uploaded
func validateRequest(r *http.Request) error {
	if r.Body == http.NoBody {
		return errors.New("request has no body")
	}
	contentType := r.Header.Get(contentTypeHeader)
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return err
	}
	if mediaType != contentTypeTar {
		return errors.New("content-type not set to tar")
	}
	return nil
}

// storeDir stores all files recursively contained in the directory given as a tar
// it returns the hash for the uploaded manifest corresponding to the uploaded dir
func storeDir(ctx context.Context, encrypt bool, reader io.ReadCloser, log logging.Logger, p pipelineFunc, ls file.LoadSaver, indexFilename string, errorFilename string) (boson.Address, error) {
	logger := tracing.NewLoggerWithTraceID(ctx, log)

	dirManifest, err := manifest.NewDefaultManifest(ls, encrypt)
	if err != nil {
		return boson.ZeroAddress, err
	}

	if indexFilename != "" && strings.ContainsRune(indexFilename, '/') {
		return boson.ZeroAddress, fmt.Errorf("index document suffix must not include slash character")
	}

	// set up HTTP body reader
	tarReader := tar.NewReader(reader)
	defer reader.Close()

	filesAdded := 0

	// iterate through the files in the supplied tar
	for {
		fileHeader, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return boson.ZeroAddress, fmt.Errorf("read tar stream: %w", err)
		}

		filePath := filepath.Clean(fileHeader.Name)

		if filePath == "." {
			logger.Warning("skipping file upload empty path")
			continue
		}

		if runtime.GOOS == "windows" {
			// always use Unix path separator
			filePath = filepath.ToSlash(filePath)
		}

		// only store regular files
		if !fileHeader.FileInfo().Mode().IsRegular() {
			logger.Warningf("skipping file upload for %s as it is not a regular file", filePath)
			continue
		}

		fileName := fileHeader.FileInfo().Name()
		contentType := mime.TypeByExtension(filepath.Ext(fileHeader.Name))

		// upload file
		fileInfo := &fileUploadInfo{
			name:        fileName,
			size:        fileHeader.FileInfo().Size(),
			contentType: contentType,
			reader:      tarReader,
		}

		fileReference, err := storeFile(ctx, fileInfo, p, encrypt)
		if err != nil {
			return boson.ZeroAddress, fmt.Errorf("store dir file: %w", err)
		}
		logger.Tracef("uploaded dir file %v with reference %v", filePath, fileReference)

		// add file entry to dir manifest
		err = dirManifest.Add(ctx, filePath, manifest.NewEntry(fileReference, nil))
		if err != nil {
			return boson.ZeroAddress, fmt.Errorf("add to manifest: %w", err)
		}

		filesAdded++
	}

	// check if files were uploaded through the manifest
	if filesAdded == 0 {
		return boson.ZeroAddress, fmt.Errorf("no files in tar")
	}

	// store website information
	if indexFilename != "" || errorFilename != "" {
		metadata := map[string]string{}
		if indexFilename != "" {
			metadata[manifestWebsiteIndexDocumentSuffixKey] = indexFilename
		}
		if errorFilename != "" {
			metadata[manifestWebsiteErrorDocumentPathKey] = errorFilename
		}
		rootManifestEntry := manifest.NewEntry(boson.ZeroAddress, metadata)
		err = dirManifest.Add(ctx, manifestRootPath, rootManifestEntry)
		if err != nil {
			return boson.ZeroAddress, fmt.Errorf("add to manifest: %w", err)
		}
	}

	storeSizeFn := []manifest.StoreSizeFunc{}

	// save manifest
	manifestBytesReference, err := dirManifest.Store(ctx, storeSizeFn...)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("store manifest: %w", err)
	}

	// store the manifest metadata and get its reference
	m := entry.NewMetadata(manifestBytesReference.String())
	m.MimeType = dirManifest.Type()
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("metadata marshal: %w", err)
	}

	mr, err := p(ctx, bytes.NewReader(metadataBytes), int64(len(metadataBytes)))
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("split metadata: %w", err)
	}

	// now join both references (fr, mr) to create an entry and store it
	e := entry.New(manifestBytesReference, mr)
	fileEntryBytes, err := e.MarshalBinary()
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("entry marshal: %w", err)
	}

	manifestFileReference, err := p(ctx, bytes.NewReader(fileEntryBytes), int64(len(fileEntryBytes)))
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("split entry: %w", err)
	}

	return manifestFileReference, nil
}

// storeFile uploads the given file and returns its reference
// this function was extracted from `fileUploadHandler` and should eventually replace its current code
func storeFile(ctx context.Context, fileInfo *fileUploadInfo, p pipelineFunc, encrypt bool) (boson.Address, error) {
	// first store the file and get its reference
	fr, err := p(ctx, fileInfo.reader, fileInfo.size)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("split file: %w", err)
	}

	// if filename is still empty, use the file hash as the filename
	if fileInfo.name == "" {
		fileInfo.name = fr.String()
	}

	// then store the metadata and get its reference
	m := entry.NewMetadata(fileInfo.name)
	m.MimeType = fileInfo.contentType
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("metadata marshal: %w", err)
	}

	mr, err := p(ctx, bytes.NewReader(metadataBytes), int64(len(metadataBytes)))
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("split metadata: %w", err)
	}

	// now join both references (mr, fr) to create an entry and store it
	e := entry.New(fr, mr)
	fileEntryBytes, err := e.MarshalBinary()
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("entry marshal: %w", err)
	}
	ref, err := p(ctx, bytes.NewReader(fileEntryBytes), int64(len(fileEntryBytes)))
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("split entry: %w", err)
	}

	return ref, nil
}

func (s *server) dirDeleteHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("delete aurora: parse address: %w", err)
		s.logger.Errorf("delete aurora: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	// MUST request local db
	r = r.WithContext(sctx.SetRootCID(sctx.SetLocalGet(r.Context()), hash))

	// There is no direct return success.
	_, err = s.storer.Get(r.Context(), storage.ModeGetRequest, hash)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			jsonhttp.NotFound(w, nil)
			return
		}

		s.logger.Debugf("delete aurora: check %s exists: %w", hash, err)
		s.logger.Errorf("delete aurora: check %s exists", hash)
		jsonhttp.InternalServerError(w, err)
		return
	}

	pyramid := s.chunkInfo.GetChunkPyramid(hash)
	chunkHashes := make([]chunkinfo.PyramidCidNum, len(pyramid))

	for i, chunk := range pyramid {
		chunkHashes[i] = *chunk
	}

	ok := s.chunkInfo.DelFile(hash)
	if !ok {
		s.logger.Errorf("delete aurora: chunk info report remove %s failed", hash)
		jsonhttp.InternalServerError(w, "dirs deleting occur error")
		return
	}

	for _, chunk := range chunkHashes {
		if chunk.Cid.Equal(hash) {
			continue
		}

		for i := 0; i < chunk.Number; i++ {
			err = s.storer.Set(r.Context(), storage.ModeSetRemove, chunk.Cid)
			if err != nil {
				if errors.Is(err, leveldb.ErrNotFound) {
					continue
				}

				s.logger.Debugf("delete aurora: remove chunk: %w", err)
				s.logger.Errorf("delete aurora: remove chunk %s", chunk.Cid)
				jsonhttp.InternalServerError(w, "dirs deletion occur error")
				return
			}
		}
	}

	err = s.storer.Set(r.Context(), storage.ModeSetRemove, hash)
	if err != nil {
		if !errors.Is(err, leveldb.ErrNotFound) {
			s.logger.Debugf("delete aurora: remove chunk: %w", err)
			s.logger.Errorf("delete aurora: remove chunk %s", hash)
			jsonhttp.InternalServerError(w, "dirs deletion occur error")
			return
		}
	}

	ok = s.chunkInfo.DelPyramid(hash)
	if !ok {
		s.logger.Errorf("delete aurora: chunk info report delete %s related pyramid failed", hash)
		jsonhttp.InternalServerError(w, "dirs deleting occur error")
		return
	}

	jsonhttp.OK(w, nil)
}

func (s *server) serveManifestMetadata(w http.ResponseWriter, r *http.Request, e *entry.Entry) {
	var (
		logger = tracing.NewLoggerWithTraceID(r.Context(), s.logger)
		ctx    = r.Context()
		buf    = bytes.NewBuffer(nil)
	)

	// read metadata
	j, _, err := joiner.New(ctx, s.storer, e.Metadata())
	if err != nil {
		logger.Debugf("manifest view: joiner %s: %v", e.Metadata(), err)
		logger.Errorf("manifest view: joiner %s", e.Metadata())
		jsonhttp.NotFound(w, nil)
		return
	}

	buf = bytes.NewBuffer(nil)
	_, err = file.JoinReadAll(ctx, j, buf)
	if err != nil {
		logger.Debugf("manifest view: read metadata %s: %v", e.Metadata(), err)
		logger.Errorf("manifest view: read metadata %s", e.Metadata())
		jsonhttp.NotFound(w, nil)
		return
	}

	metadata := &entry.Metadata{}
	err = json.Unmarshal(buf.Bytes(), metadata)
	if err != nil {
		logger.Debugf("manifest view: unmarshal metadata %s: %v", e.Metadata(), err)
		logger.Errorf("manifest view: unmarshal metadata %s", e.Metadata())
		jsonhttp.NotFound(w, nil)
		return
	}

	refChunk, err := s.storer.Get(ctx, storage.ModeGetRequest, e.Reference())
	if err != nil {
		jsonhttp.NotFound(w, nil)
		return
	}

	if len(refChunk.Data()) < boson.SpanSize {
		s.logger.Error("manifest view: chunk data too short")
		jsonhttp.BadRequest(w, "short chunk data")
		return
	}

	var ext string
	lastDot := strings.LastIndexByte(metadata.Filename, '.')
	if lastDot != -1 {
		ext = metadata.Filename[lastDot:]
	}

	type FsType struct {
		Name      string `json:"name"`
		Size      uint64 `json:"size"`
		Extension string `json:"ext"`
		MimeType  string `json:"mime"`
	}

	jsonhttp.OK(w, &FsType{
		Name:      metadata.Filename,
		Size:      binary.LittleEndian.Uint64(refChunk.Data()[:boson.SpanSize]),
		Extension: ext,
		MimeType:  metadata.MimeType,
	})
}

func (s *server) manifestViewHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	ls := loadsave.NewReadonly(s.storer)

	nameOrHex := mux.Vars(r)["address"]
	pathVar := mux.Vars(r)["path"]

	depth := 1
	if r.URL.Query().Get("recursive") != "" {
		depth = -1
	}

	// force enable
	depth = -1

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("manifest view: parse address %s: %v", nameOrHex, err)
		logger.Error("manifest view: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}

	// read manifest entry
	j, _, err := joiner.New(r.Context(), s.storer, address)
	if err != nil {
		logger.Debugf("manifest view: joiner manifest entry %s: %v", address, err)
		logger.Errorf("manifest view: joiner %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	buf := bytes.NewBuffer(nil)
	_, err = file.JoinReadAll(r.Context(), j, buf)
	if err != nil {
		logger.Debugf("manifest view: read entry %s: %v", address, err)
		logger.Errorf("manifest view: read entry %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	e := &entry.Entry{}
	err = e.UnmarshalBinary(buf.Bytes())
	if err != nil {
		logger.Debugf("manifest view: unmarshal entry %s: %v", address, err)
		logger.Errorf("manifest view: unmarshal entry %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	// read metadata
	j, _, err = joiner.New(r.Context(), s.storer, e.Metadata())
	if err != nil {
		logger.Debugf("manifest view: joiner metadata %s: %v", address, err)
		logger.Errorf("manifest view: joiner %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	// read metadata
	buf = bytes.NewBuffer(nil)
	_, err = file.JoinReadAll(r.Context(), j, buf)
	if err != nil {
		logger.Debugf("manifest view: read metadata %s: %v", address, err)
		logger.Errorf("manifest view: read metadata %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	metadata := &entry.Metadata{}
	err = json.Unmarshal(buf.Bytes(), metadata)
	if err != nil {
		logger.Debugf("manifest view: unmarshal metadata %s: %v", address, err)
		logger.Errorf("manifest view: unmarshal metadata %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	var fe *entry.Entry

	switch metadata.MimeType {
	case manifest.ManifestSimpleContentType, manifest.ManifestMantarayContentType:
		m, err := manifest.NewManifestReference(
			metadata.MimeType,
			e.Reference(),
			ls,
		)
		if err != nil {
			logger.Debugf("manifest view: load manifest %s: %v", address, err)
			logger.Error("manifest view: load manifest")
			jsonhttp.NotFound(w, nil)
			return
		}

		if pathVar == "" || strings.HasSuffix(pathVar, "/") {
			type FsNode struct {
				Type  string             `json:"type"`
				Hash  string             `json:"hash,omitempty"`
				Nodes map[string]*FsNode `json:"sub,omitempty"`
			}

			rootNode := &FsNode{
				Type: manifest.Directory.String(),
			}

			findNode := func(n *FsNode, path []byte) *FsNode {
				i := 0
				p := n
				for j := 0; j < len(path); j++ {
					if path[j] == '/' {
						if p.Nodes == nil {
							p.Nodes = make(map[string]*FsNode)
						}
						dir := path[i:j]
						sub, ok := p.Nodes[string(dir)]
						if !ok {
							p.Nodes[string(dir)] = &FsNode{
								Type: manifest.Directory.String(),
							}
							sub = p.Nodes[string(dir)]
						}
						p = sub
						i = j + 1
					}
				}
				return p
			}

			fn := func(nodeType int, path, prefix, hash []byte) error {
				node := findNode(rootNode, path)
				if nodeType == int(manifest.File) {
					if node.Nodes == nil {
						node.Nodes = make(map[string]*FsNode)
					}
					node.Nodes[string(prefix)] = &FsNode{
						Type: manifest.File.String(),
						Hash: boson.NewAddress(hash).String(),
					}
				}

				return nil
			}

			pathVar = strings.TrimSuffix(pathVar, "/")

			if err := m.IterateNodes(r.Context(), []byte(pathVar), depth, fn); err != nil {
				jsonhttp.InternalServerError(w, err)
				return
			}

			if indexDocumentSuffixKey, ok := manifestMetadataLoad(r.Context(), m, manifestRootPath, manifestWebsiteIndexDocumentSuffixKey); ok {
				_, err := m.Lookup(r.Context(), indexDocumentSuffixKey)
				if err != nil {
					logger.Debugf("manifest view: invalid index %s/%s: %v", address, indexDocumentSuffixKey, err)
					logger.Error("manifest view: invalid index")
				}

				indexNode, ok := rootNode.Nodes[indexDocumentSuffixKey]
				if ok && indexNode.Type == manifest.File.String() {
					indexNode.Type = manifest.IndexItem.String()
				}
			}

			jsonhttp.OK(w, rootNode)
			return
		}

		me, err := m.Lookup(r.Context(), pathVar)
		if err != nil {
			logger.Debugf("manifest view: invalid path %s/%s: %v", address, pathVar, err)
			logger.Error("manifest view: invalid path")
			jsonhttp.NotFound(w, nil)
			return
		}

		reference := me.Reference()

		// read file entry
		j, _, err := joiner.New(r.Context(), s.storer, reference)
		if err != nil {
			logger.Debugf("manifest view: joiner read file entry %s: %v", address, err)
			logger.Errorf("manifest view: joiner read file entry %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}

		buf = bytes.NewBuffer(nil)
		_, err = file.JoinReadAll(r.Context(), j, buf)
		if err != nil {
			logger.Debugf("manifest view: read file entry %s: %v", address, err)
			logger.Errorf("manifest view: read file entry %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}
		fe = &entry.Entry{}
		err = fe.UnmarshalBinary(buf.Bytes())
		if err != nil {
			logger.Debugf("manifest view: unmarshal file entry %s: %v", address, err)
			logger.Errorf("manifest view: unmarshal file entry %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}
	default:
		// read entry
		j, _, err := joiner.New(r.Context(), s.storer, address)
		if err != nil {
			errors.Is(err, storage.ErrNotFound)
			logger.Debugf("manifest view: joiner %s: %v", address, err)
			logger.Errorf("manifest view: joiner %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}

		buf := bytes.NewBuffer(nil)
		_, err = file.JoinReadAll(r.Context(), j, buf)
		if err != nil {
			logger.Debugf("manifest view: read entry %s: %v", address, err)
			logger.Errorf("manifest view: read entry %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}
		fe = &entry.Entry{}
		err = fe.UnmarshalBinary(buf.Bytes())
		if err != nil {
			logger.Debugf("manifest view: unmarshal entry %s: %v", address, err)
			logger.Errorf("manifest view: unmarshal entry %s", address)
			jsonhttp.NotFound(w, nil)
			return
		}
	}

	s.serveManifestMetadata(w, r, fe)
}
