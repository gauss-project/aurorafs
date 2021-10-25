package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethersphere/langos"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
)

func (s *server) auroraUploadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)

	contentType := r.Header.Get(contentTypeHeader)
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		logger.Debugf("aurora upload: parse content type header %q: %v", contentType, err)
		logger.Errorf("aurora upload: parse content type header %q", contentType)
		jsonhttp.BadRequest(w, invalidContentType)
		return
	}
	isDir := r.Header.Get(AuroraCollectionHeader)
	if strings.ToLower(isDir) == "true" || mediaType == multiPartFormData {
		s.dirUploadHandler(w, r)
		return
	}
	s.fileUploadHandler(w, r)
}

// auroraUploadResponse is returned when an HTTP request to upload a file or collection is successful
type auroraUploadResponse struct {
	Reference boson.Address `json:"reference"`
}

// fileUploadHandler uploads the file and its metadata supplied in the file body and
// the headers
func (s *server) fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	var (
		reader            io.Reader
		dirName, fileName string
	)

	// Content-Type has already been validated by this time
	contentType := r.Header.Get(contentTypeHeader)

	ctx := r.Context()

	fileName = r.URL.Query().Get("name")
	dirName = r.Header.Get(AuroraCollectionNameHeader)
	reader = r.Body

	p := requestPipelineFn(s.storer, r)

	// first store the file and get its reference
	fr, err := p(ctx, reader)
	if err != nil {
		logger.Debugf("aurora upload file: file store, file %q: %v", fileName, err)
		logger.Errorf("aurora upload file: file store, file %q", fileName)
		jsonhttp.InternalServerError(w, fileStoreError)
		return
	}

	// If filename is still empty, use the file hash as the filename
	if fileName == "" {
		fileName = fr.String()
	}

	encrypt := requestEncrypt(r)
	factory := requestPipelineFactory(ctx, s.storer, r)
	l := loadsave.New(s.storer, factory)

	m, err := manifest.NewDefaultManifest(l, encrypt)
	if err != nil {
		logger.Debugf("aurora upload file: create manifest, file %q: %v", fileName, err)
		logger.Errorf("aurora upload file: create manifest, file %q", fileName)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	rootMtdt := map[string]string{
		manifest.WebsiteIndexDocumentSuffixKey: fileName,
	}

	if dirName != "" {
		rootMtdt[manifest.EntryMetadataDirnameKey] = dirName
	}

	err = m.Add(ctx, manifest.RootPath, manifest.NewEntry(boson.ZeroAddress, rootMtdt))
	if err != nil {
		logger.Debugf("aurora upload file: adding metadata to manifest, file %q: %v", fileName, err)
		logger.Errorf("aurora upload file: adding metadata to manifest, file %q", fileName)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	fileMtdt := map[string]string{
		manifest.EntryMetadataContentTypeKey: contentType,
		manifest.EntryMetadataFilenameKey:    fileName,
	}

	err = m.Add(ctx, fileName, manifest.NewEntry(fr, fileMtdt))
	if err != nil {
		logger.Debugf("aurora upload file: adding file to manifest, file %q: %v", fileName, err)
		logger.Errorf("aurora upload file: adding file to manifest, file %q", fileName)
		jsonhttp.InternalServerError(w, nil)
		return
	}

	logger.Debugf("Uploading file Encrypt: %v Filename: %s Filehash: %s FileMtdt: %v",
		encrypt, fileName, fr.String(), fileMtdt)

	var storeSizeFn []manifest.StoreSizeFunc

	manifestReference, err := m.Store(ctx, storeSizeFn...)
	if err != nil {
		logger.Debugf("aurora upload file: manifest store, file %q: %v", fileName, err)
		logger.Errorf("aurora upload file: manifest store, file %q", fileName)
		jsonhttp.InternalServerError(w, nil)
		return
	}
	logger.Debugf("Manifest Reference: %s", manifestReference.String())

	pyramid, err := s.traversal.GetPyramid(ctx, manifestReference)
	if err != nil {
		logger.Errorf("file upload: get trie data, file %q: %v", fileName, err)
		jsonhttp.InternalServerError(w, "could not get trie data")
		return
	}

	dataChunks, _ := s.traversal.GetChunkHashes(ctx, manifestReference, pyramid)
	if err != nil {
		logger.Errorf("file upload: check trie data, file %q: %v", fileName, err)
		jsonhttp.InternalServerError(w, "check trie data error")
		return
	}

	for _, li := range dataChunks {
		for _, b := range li {
			err := s.chunkInfo.OnChunkTransferred(boson.NewAddress(b), manifestReference, s.overlay, boson.ZeroAddress)
			if err != nil {
				logger.Errorf("chunk transfer data err: %v", err)
				jsonhttp.InternalServerError(w, "chunk transfer data error")
				return
			}
		}
	}

	if strings.ToLower(r.Header.Get(AuroraPinHeader)) == "true" {
		if err := s.pinning.CreatePin(ctx, manifestReference, false); err != nil {
			logger.Debugf("bzz upload file: creation of pin for %q failed: %v", manifestReference, err)
			logger.Error("bzz upload file: creation of pin failed")
			jsonhttp.InternalServerError(w, nil)
			return
		}
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", manifestReference.String()))
	jsonhttp.Created(w, auroraUploadResponse{
		Reference: manifestReference,
	})
}

func (s *server) auroraDownloadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	ls := loadsave.NewReadonly(s.storer)

	targets := r.URL.Query().Get("targets")
	if targets != "" {
		r = r.WithContext(sctx.SetTargets(r.Context(), targets))
	}

	nameOrHex := mux.Vars(r)["address"]
	pathVar := mux.Vars(r)["path"]
	if strings.HasSuffix(pathVar, "/") {
		pathVar = strings.TrimRight(pathVar, "/")
		// NOTE: leave one slash if there was some
		pathVar += "/"
	}

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("aurora download: parse address %s: %v", nameOrHex, err)
		logger.Error("aurora download: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}

	r = r.WithContext(sctx.SetRootCID(r.Context(), address))
	if !s.chunkInfo.Init(r.Context(), nil, address) {
		logger.Debugf("aurora download: chunkInfo init %s: %v", nameOrHex, err)
		jsonhttp.NotFound(w, nil)
		return
	}

	ctx := r.Context()

	// read manifest entry
	m, err := manifest.NewDefaultManifestReference(
		address,
		ls,
	)
	if err != nil {
		logger.Debugf("aurora download: not manifest %s: %v", address, err)
		logger.Errorf("aurora download: not manifest %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	if pathVar == "" {
		logger.Tracef("aurora download: handle empty path %s", address)

		if indexDocumentSuffixKey, ok := manifestMetadataLoad(ctx, m, manifest.RootPath, manifest.WebsiteIndexDocumentSuffixKey); ok {
			pathWithIndex := path.Join(pathVar, indexDocumentSuffixKey)
			indexDocumentManifestEntry, err := m.Lookup(ctx, pathWithIndex)
			if err == nil {
				// index document exists
				logger.Debugf("aurora download: serving path: %s", pathWithIndex)

				s.serveManifestEntry(w, r, address, indexDocumentManifestEntry, true)
				return
			}
		}
	}

	me, err := m.Lookup(ctx, pathVar)
	if err != nil {
		logger.Debugf("aurora download: invalid path %s/%s: %v", address, pathVar, err)
		logger.Error("aurora download: invalid path")

		if errors.Is(err, manifest.ErrNotFound) {

			if !strings.HasPrefix(pathVar, "/") {
				// check for directory
				dirPath := pathVar + "/"
				exists, err := m.HasPrefix(r.Context(), dirPath)
				if err == nil && exists {
					// redirect to directory
					u := r.URL
					u.Path += "/"
					redirectURL := u.String()

					logger.Debugf("aurora download: redirecting to %s: %v", redirectURL, err)

					http.Redirect(w, r, redirectURL, http.StatusPermanentRedirect)
					return
				}
			}

			// check index suffix path
			if indexDocumentSuffixKey, ok := manifestMetadataLoad(r.Context(), m, manifest.RootPath, manifest.WebsiteIndexDocumentSuffixKey); ok {
				if !strings.HasSuffix(pathVar, indexDocumentSuffixKey) {
					// check if path is directory with index
					pathWithIndex := path.Join(pathVar, indexDocumentSuffixKey)
					indexDocumentManifestEntry, err := m.Lookup(r.Context(), pathWithIndex)
					if err == nil {
						// index document exists
						logger.Debugf("aurora download: serving path: %s", pathWithIndex)

						s.serveManifestEntry(w, r, address, indexDocumentManifestEntry, true)
						return
					}
				}
			}

			// check if error document is to be shown
			if errorDocumentPath, ok := manifestMetadataLoad(r.Context(), m, manifest.RootPath, manifest.WebsiteErrorDocumentPathKey); ok {
				if pathVar != errorDocumentPath {
					errorDocumentManifestEntry, err := m.Lookup(r.Context(), errorDocumentPath)
					if err == nil {
						// error document exists
						logger.Debugf("aurora download: serving path: %s", errorDocumentPath)

						s.serveManifestEntry(w, r, address, errorDocumentManifestEntry, true)
						return
					}
				}
			}

			jsonhttp.NotFound(w, "path address not found")
		} else {
			jsonhttp.NotFound(w, nil)
		}
		return
	}

	// serve requested path
	s.serveManifestEntry(w, r, address, me, true)
}

func (s *server) serveManifestEntry(
	w http.ResponseWriter,
	r *http.Request,
	address boson.Address,
	manifestEntry manifest.Entry,
	etag bool,
) {
	additionalHeaders := http.Header{}
	metadata := manifestEntry.Metadata()
	if fname, ok := metadata[manifest.EntryMetadataFilenameKey]; ok {
		fname = filepath.Base(fname) // only keep the file name
		additionalHeaders["Content-Disposition"] =
			[]string{fmt.Sprintf("inline; filename=\"%s\"", fname)}
	}

	if mimeType, ok := metadata[manifest.EntryMetadataContentTypeKey]; ok {
		additionalHeaders["Content-Type"] = []string{mimeType}
	}

	s.downloadHandler(w, r, manifestEntry.Reference(), additionalHeaders, etag)
}

// downloadHandler contains common logic for dowloading Aurora file from API
func (s *server) downloadHandler(w http.ResponseWriter, r *http.Request, reference boson.Address, additionalHeaders http.Header, etag bool) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	targets := r.URL.Query().Get("targets")
	if targets != "" {
		r = r.WithContext(sctx.SetTargets(r.Context(), targets))
	}

	reader, l, err := joiner.New(r.Context(), s.storer, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.chunkInfo.Init(r.Context(), nil, sctx.GetRootCID(r.Context()))
			logger.Debugf("api download: not found %s: %v", reference, err)
			logger.Error("api download: not found")
			jsonhttp.NotFound(w, nil)
			return
		}
		logger.Debugf("api download: unexpected error %s: %v", reference, err)
		logger.Error("api download: unexpected error")
		jsonhttp.InternalServerError(w, nil)
		return
	}

	// include additional headers
	for name, values := range additionalHeaders {
		w.Header().Set(name, strings.Join(values, "; "))
	}

	if etag {
		w.Header().Set("ETag", fmt.Sprintf("%q", reference))
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Decompressed-Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")
	if targets != "" {
		w.Header().Set(TargetsRecoveryHeader, targets)
	}
	http.ServeContent(w, r, "", time.Now(), langos.NewBufferedLangos(reader, lookaheadBufferSize(l)))
}

type auroraListResponse struct {
	FileHash  string              `json:"fileHash"`
	Size      int                 `json:"size"`
	FileSize  int                 `json:"fileSize"`
	PinState  bool                `json:"pinState"`
	BitVector aurora.BitVectorApi `json:"bitVector"`
}

// auroraListHandler
func (s *server) auroraListHandler(w http.ResponseWriter, r *http.Request) {
	responseList := make([]auroraListResponse, 0)

	fileListInfo, addressList := s.chunkInfo.GetFileList(s.overlay)

	if len(fileListInfo) > 0 && len(addressList) > 0 {
		yes, err := s.storer.HasMulti(context.Background(), storage.ModeHasPin, addressList...)
		if err != nil {
			return
		}
		for i, v := range addressList {
			//fileListInfo[v.String()].PinState = yes[i]
			Response := auroraListResponse{}
			Response.FileHash = v.String()
			Response.FileSize = fileListInfo[v.String()].FileSize
			Response.Size = fileListInfo[v.String()].TreeSize
			Response.PinState = yes[i]
			Response.BitVector = fileListInfo[v.String()].Bitvector
			responseList = append(responseList, Response)
		}
		if len(responseList) > 0 {
			sort.Slice(responseList, func(i, j int) bool {
				return responseList[i].FileHash < responseList[j].FileHash
			})
		}

	}
	jsonhttp.OK(w, responseList)
}

// manifestMetadataLoad returns the value for a key stored in the metadata of
// manifest path, or empty string if no value is present.
// The ok result indicates whether value was found in the metadata.
func manifestMetadataLoad(
	ctx context.Context,
	manifest manifest.Interface,
	path, metadataKey string,
) (string, bool) {
	me, err := manifest.Lookup(ctx, path)
	if err != nil {
		return "", false
	}

	manifestRootMetadata := me.Metadata()
	if val, ok := manifestRootMetadata[metadataKey]; ok {
		return val, ok
	}

	return "", false
}

type ManifestNode struct {
	Type      string                   `json:"type"`
	Hash      string                   `json:"hash,omitempty"`
	Name      string                   `json:"name,omitempty"`
	Size      uint64                   `json:"size,omitempty"`
	Extension string                   `json:"ext,omitempty"`
	MimeType  string                   `json:"mime,omitempty"`
	Nodes     map[string]*ManifestNode `json:"sub,omitempty"`
}

// manifestViewHandler
func (s *server) manifestViewHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	ls := loadsave.NewReadonly(s.storer)

	nameOrHex := mux.Vars(r)["address"]
	pathVar := mux.Vars(r)["path"]

	depth := 1
	if r.URL.Query().Get("recursive") != "" {
		depth = -1
	}

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("manifest view: parse address %s: %v", nameOrHex, err)
		logger.Error("manifest view: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}

	// read manifest entry
	m, err := manifest.NewDefaultManifestReference(
		address,
		ls,
	)
	if err != nil {
		logger.Debugf("aurora download: not manifest %s: %v", address, err)
		logger.Errorf("aurora download: not manifest %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	ctx := r.Context()
	rootNode := &ManifestNode{
		Type:  manifest.Directory.String(),
		Nodes: make(map[string]*ManifestNode),
	}

	if pathVar == "" || strings.HasSuffix(pathVar, "/") {
		findNode := func(n *ManifestNode, path []byte) *ManifestNode {
			i := 0
			p := n
			for j := 0; j < len(path); j++ {
				if path[j] == '/' {
					if p.Nodes == nil {
						p.Nodes = make(map[string]*ManifestNode)
					}
					dir := path[i:j]
					sub, ok := p.Nodes[string(dir)]
					if !ok {
						p.Nodes[string(dir)] = &ManifestNode{
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

		fn := func(nodeType int, path, prefix, hash []byte, metadata map[string]string) error {
			if bytes.Equal(path, []byte("/")) {
				rootNode.Name = metadata[manifest.EntryMetadataDirnameKey]

				return nil
			}

			node := findNode(rootNode, path)

			switch nodeType {
			case int(manifest.Directory):
				if node.Nodes == nil {
					node.Nodes = make(map[string]*ManifestNode)
				}
			case int(manifest.File):
				filename := metadata[manifest.EntryMetadataFilenameKey]
				extension := ""

				lastDot := strings.LastIndexByte(filename, '.')
				if lastDot != -1 {
					extension = filename[lastDot:]
				}

				refChunk, err := s.storer.Get(ctx, storage.ModeGetRequest, boson.NewAddress(hash))
				if err != nil {
					return err
				}

				node.Nodes[string(prefix)] = &ManifestNode{
					Type:      manifest.File.String(),
					Hash:      boson.NewAddress(hash).String(),
					Size:      binary.LittleEndian.Uint64(refChunk.Data()[:boson.SpanSize]),
					Extension: extension,
					MimeType:  metadata[manifest.EntryMetadataContentTypeKey],
				}
			}

			return nil
		}

		pathVar = strings.TrimSuffix(pathVar, "/")
		if pathVar != "" {
			pathVar += "/"
		}

		if err := m.IterateNodes(ctx, []byte(pathVar), depth, fn); err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
	} else {
		e, err := m.Lookup(ctx, pathVar)
		if err != nil {
			logger.Debugf("manifest view: invalid filename: %v", err)
			logger.Error("manifest view: invalid filename")
			jsonhttp.NotFound(w, nil)
			return
		}

		filename := e.Metadata()[manifest.EntryMetadataFilenameKey]
		extension := ""

		lastDot := strings.LastIndexByte(filename, '.')
		if lastDot != -1 {
			extension = filename[lastDot:]
		}

		refChunk, err := s.storer.Get(ctx, storage.ModeGetRequest, e.Reference())
		if err != nil {
			logger.Debugf("manifest view: file not found: %v", err)
			logger.Error("manifest view: file not found")
			jsonhttp.NotFound(w, nil)
			return
		}

		rootNode.Name = e.Metadata()[manifest.EntryMetadataDirnameKey]
		rootNode.Nodes = map[string]*ManifestNode{
			filename: {
				Type:      manifest.File.String(),
				Hash:      e.Reference().String(),
				Size:      binary.LittleEndian.Uint64(refChunk.Data()[:boson.SpanSize]),
				Extension: extension,
				MimeType:  e.Metadata()[manifest.EntryMetadataContentTypeKey],
			},
		}
	}

	if indexDocumentSuffixKey, ok := manifestMetadataLoad(ctx, m, manifest.RootPath, manifest.WebsiteIndexDocumentSuffixKey); ok {
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
}
