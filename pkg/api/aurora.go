package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/langos"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/fileinfo"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/localstore/filestore"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
)

var (
	ErrNotFound    = errors.New("manifest: not found")
	ErrServerError = errors.New("manifest: ServerError")
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
	if strings.ToLower(isDir) == StringTrue || mediaType == multiPartFormData {
		s.dirUploadHandler(w, r)
		return
	}
	s.fileUploadHandler(w, r)
}

// auroraUploadResponse is returned when an HTTP request to upload a file or collection is successful
type auroraUploadResponse struct {
	Reference boson.Address `json:"reference"`
}

type auroraRegisterResponse struct {
	Hash common.Hash `json:"hash"`
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

	realIndexFilename, err := UnescapeUnicode(fileName)
	if err != nil {
		logger.Debugf("aurora upload file: filename %q unescape err: %v", fileName, err)
		logger.Errorf("aurora upload file: filename %q unescape err", fileName)
		jsonhttp.BadRequest(w, nil)
		return
	}

	rootMtdt := map[string]string{
		manifest.WebsiteIndexDocumentSuffixKey: realIndexFilename,
	}

	if dirName != "" {
		realDirName, err := UnescapeUnicode(dirName)
		if err != nil {
			logger.Debugf("aurora upload file: dirname %q unescape err: %v", dirName, err)
			logger.Errorf("aurora upload file: dirname %q unescape err", dirName)
			jsonhttp.BadRequest(w, nil)
			return
		}
		rootMtdt[manifest.EntryMetadataDirnameKey] = realDirName
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
		manifest.EntryMetadataFilenameKey:    realIndexFilename,
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

	bitLen, err := s.fileInfo.GetFileSize(manifestReference)
	if err != nil {
		logger.Errorf("aurora upload file: file size err:%v", err)
	}
	err = s.chunkInfo.OnFileUpload(ctx, manifestReference, bitLen)
	if err != nil {
		logger.Debugf("aurora upload file: chunk transfer data err: %v", err)
		logger.Errorf("aurora upload file: chunk transfer data err")
		jsonhttp.InternalServerError(w, "chunk transfer data error")
		return
	}

	if strings.ToLower(r.Header.Get(AuroraPinHeader)) == StringTrue {
		if err := s.pinning.CreatePin(ctx, manifestReference, false); err != nil {
			logger.Debugf("aurora upload file: creation of pin for %q failed: %v", manifestReference, err)
			logger.Error("aurora upload file: creation of pin failed")
			jsonhttp.InternalServerError(w, nil)
			return
		}
		err = s.fileInfo.PinFile(manifestReference, true)
		if err != nil {
			s.logger.Errorf("aurora upload file:update fileinfo pin failed:%v", err)
		}
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", manifestReference.String()))
	jsonhttp.Created(w, auroraUploadResponse{
		Reference: manifestReference,
	})
}

func (s *server) auroraDownloadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	ls := loadsave.NewReadonly(s.storer, storage.ModeGetRequest)

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

	r = r.WithContext(sctx.SetRootHash(r.Context(), address))
	if !s.chunkInfo.Discover(r.Context(), nil, address) {
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
		logger.Debugf("aurora download: handle empty path %s", address)

		if indexDocumentSuffixKey, ok := manifestMetadataLoad(ctx, m, manifest.RootPath, manifest.WebsiteIndexDocumentSuffixKey); ok {
			pathVar = path.Join(pathVar, indexDocumentSuffixKey)
			indexDocumentManifestEntry, err := m.Lookup(ctx, pathVar)
			if err == nil {
				// index document exists
				logger.Debugf("aurora download: serving path: %s", pathVar)

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
	_ boson.Address,
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

	reader, l, err := joiner.New(r.Context(), s.storer, storage.ModeGetRequest, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
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

	// http cache policy
	w.Header().Set("Cache-Control", "no-store")

	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Decompressed-Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")
	if targets != "" {
		w.Header().Set(TargetsRecoveryHeader, targets)
	}
	http.ServeContent(w, r, "", time.Now(), langos.NewBufferedLangos(reader, lookaheadBufferSize(l)))
}

type auroraListResponse struct {
	RootCid   boson.Address          `json:"rootCid"`
	Size      int                    `json:"size"`
	FileSize  int                    `json:"fileSize"`
	PinState  bool                   `json:"pinState"`
	BitVector aurora.BitVectorApi    `json:"bitVector"`
	Register  bool                   `json:"register"`
	Manifest  *fileinfo.ManifestNode `json:"manifest"`
}

type auroraPageResponse struct {
	Total int                  `json:"total"`
	List  []auroraListResponse `json:"list"`
}

func (s *server) auroraListHandler(w http.ResponseWriter, r *http.Request) {
	var wg sync.WaitGroup
	var reqs aurora.ApiBody
	isBody := true
	isPage := false
	pageTotal := 0

	req, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("aurora list: Request parameter acquisition failed,%v", err.Error())
		jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Request parameter acquisition failed,%v", err.Error()))
		return
	}

	if len(req) == 0 {
		isBody = false
	}

	page := r.URL.Query().Get("page")
	if page != "" {
		isPage = true
		var apiPage aurora.ApiPage
		err := json.Unmarshal([]byte(page), &apiPage)
		if err != nil {
			s.logger.Error("aurora list: Request parameter conversion failed,%v", err.Error())
			jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Request parameter conversion failed,%v", err.Error()))
			return
		}
		reqs.Page = apiPage
	}
	filter := r.URL.Query().Get("filter")
	if filter != "" {
		isPage = true
		apiFilters := make([]aurora.ApiFilter, 0, 7)
		err := json.Unmarshal([]byte(filter), &apiFilters)
		if err != nil {
			s.logger.Error("aurora list: Request parameter conversion failed,%v", err.Error())
			jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Request parameter conversion failed,%v", err.Error()))
			return
		}
		reqs.Filter = apiFilters
	}
	asort := r.URL.Query().Get("sort")
	if asort != "" {
		isPage = true
		var apiSort aurora.ApiSort
		err := json.Unmarshal([]byte(asort), &apiSort)
		if err != nil {
			s.logger.Error("aurora list: Request parameter conversion failed,%v", err.Error())
			jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Request parameter conversion failed,%v", err.Error()))
			return
		}
		reqs.Sort = apiSort
	}
	if isBody {
		isPage = true
		err = json.Unmarshal(req, &reqs)
		if err != nil {
			s.logger.Error("aurora list: Request parameter conversion failed,%v", err.Error())
			jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Request parameter conversion failed,%v", err.Error()))
			return
		}
	}
	if isPage {
		if reqs.Page.PageSize == reqs.Page.PageNum && reqs.Page.PageSize == 0 {
			jsonhttp.InternalServerError(w, fmt.Errorf("aurora list: Page information error"))
			return
		}
	}

	//if r.URL.Query().Get("recursive") != "" {
	//	depth = -1
	//}

	var filePage filestore.Page
	var fileFilter []filestore.Filter
	var fileSort filestore.Sort
	if isPage {
		filePage = filestore.Page{
			PageNum:  reqs.Page.PageNum,
			PageSize: reqs.Page.PageSize,
		}
		fileFilter = make([]filestore.Filter, 0, len(reqs.Filter))
		for i := range reqs.Filter {
			fileFilter = append(fileFilter, filestore.Filter{
				Key:   reqs.Filter[i].Key,
				Term:  reqs.Filter[i].Term,
				Value: reqs.Filter[i].Value,
			})
		}
		fileSort = filestore.Sort{
			Key:   reqs.Sort.Key,
			Order: reqs.Sort.Order,
		}

	}
	fileListInfo := s.fileInfo.GetFileList(filePage, fileFilter, fileSort)

	responseList := make([]auroraListResponse, 0)

	for i := range fileListInfo {
		responseList = append(responseList, auroraListResponse{
			RootCid:  fileListInfo[i].RootCid,
			PinState: fileListInfo[i].Pinned,
			BitVector: aurora.BitVectorApi{
				Len: fileListInfo[i].BvLen,
				B:   fileListInfo[i].Bv,
			},
			Register: fileListInfo[i].Registered,
			Manifest: &fileinfo.ManifestNode{
				Type:      fileListInfo[i].Type,
				Hash:      fileListInfo[i].Hash,
				Name:      fileListInfo[i].Name,
				Size:      uint64(fileListInfo[i].Size),
				Extension: fileListInfo[i].Extension,
				MimeType:  fileListInfo[i].MimeType,
			},
		})
	}
	wg.Add(len(responseList))
	type ordHash struct {
		ord  int
		hash boson.Address
	}
	var l sync.Mutex
	ch := make(chan ordHash, len(responseList))
	getChainState := func(ctx context.Context, i int, rootCid boson.Address, workLoad chan struct{}) {
		defer wg.Done()
		defer func() {
			<-workLoad
		}()
		if v, ok := s.auroraChainSate.Load(rootCid.String()); ok {
			l.Lock()
			responseList[i].Register = v.(bool)
			l.Unlock()
			return
		}

		b, err := s.oracleChain.GetRegisterState(r.Context(), rootCid, s.overlay)
		if err != nil {
			s.logger.Errorf("aurora list: GetRegisterState failed %v", err.Error())
		}

		if err == nil && b {
			s.auroraChainSate.Store(rootCid.String(), true)
			l.Lock()
			responseList[i].Register = true
			l.Unlock()
		} else {
			s.auroraChainSate.Store(rootCid.String(), false)
		}
	}

	workLoad := make(chan struct{}, 30)
	go func() {
		for v := range ch {
			workLoad <- struct{}{}
			go getChainState(r.Context(), v.ord, v.hash, workLoad)
		}
	}()

	for i, v := range responseList {
		hashChain := ordHash{ord: i, hash: v.RootCid}
		ch <- hashChain
	}

	wg.Wait()
	close(ch)
	if !isPage {
		zeroAddress := boson.NewAddress([]byte{31: 0})
		sort.Slice(responseList, func(i, j int) bool {
			closer, _ := responseList[i].RootCid.Closer(zeroAddress, responseList[j].RootCid)
			return closer
		})
		jsonhttp.OK(w, responseList)
	} else {
		pageResponseList := auroraPageResponse{
			Total: pageTotal,
			List:  responseList,
		}
		jsonhttp.OK(w, pageResponseList)
	}

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

// manifestViewHandler
func (s *server) manifestViewHandler(w http.ResponseWriter, r *http.Request) {

	nameOrHex := mux.Vars(r)["address"]
	pathVar := mux.Vars(r)["path"]

	depth := 1
	if r.URL.Query().Get("recursive") != "" {
		depth = -1
	}

	rootNode, err := s.fileInfo.ManifestView(r.Context(), nameOrHex, pathVar, depth)
	if errors.Is(err, ErrNotFound) {
		jsonhttp.NotFound(w, nil)
	}
	if errors.Is(err, ErrServerError) {
		jsonhttp.InternalServerError(w, nil)
	}

	jsonhttp.OK(w, rootNode)
}

func (s *server) fileRegister(w http.ResponseWriter, r *http.Request) {
	apiName := "fileRegister"
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	nameOrHex := mux.Vars(r)["address"]
	address, err := s.resolveNameOrAddress(nameOrHex)
	defer s.tranProcess.Delete(apiName + address.String())

	if err != nil {
		logger.Debugf("aurora fileRegister: parse address %s: %v", nameOrHex, err)
		logger.Errorf("aurora fileRegister: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}
	if _, ok := s.tranProcess.Load(apiName + address.String()); ok {
		logger.Errorf("parse address %s under processing", nameOrHex)
		jsonhttp.InternalServerError(w, fmt.Sprintf("parse address %s under processing", nameOrHex))
		return
	}
	s.tranProcess.Store(apiName+address.String(), "-")
	overlays := s.oracleChain.GetNodesFromCid(address.Bytes())
	for _, v := range overlays {
		if s.overlay.Equal(v) {
			jsonhttp.Forbidden(w, fmt.Sprintf("address:%v Already Register", address.String()))
			return
		}
	}

	hash, err := s.oracleChain.RegisterCidAndNode(r.Context(), address, s.overlay)
	if err != nil {
		logger.Errorf("aurora fileRegister failed: %v ", err)
		jsonhttp.InternalServerError(w, fmt.Sprintf("aurora fileRegister failed: %v ", err))
		return
	}
	err = s.fileInfo.RegisterFile(address, true)
	if err != nil {
		logger.Errorf("aurora fileRegister update info:%v", err)
	}

	trans := TransactionResponse{
		Hash:     hash,
		Address:  address,
		Register: true,
	}
	s.transactionChan <- trans

	jsonhttp.OK(w,
		auroraRegisterResponse{
			Hash: hash,
		})
}

func (s *server) fileRegisterRemove(w http.ResponseWriter, r *http.Request) {
	apiName := "fileRegisterRemove"
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	nameOrHex := mux.Vars(r)["address"]
	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Errorf("aurora fileRegisterRemove: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}
	defer s.tranProcess.Delete(apiName + address.String())
	if _, ok := s.tranProcess.Load(apiName + address.String()); ok {
		logger.Errorf("parse address %s under processing", nameOrHex)
		jsonhttp.InternalServerError(w, fmt.Sprintf("parse address %s under processing", nameOrHex))
		return
	}
	s.tranProcess.Store(apiName+address.String(), "-")

	overlays := s.oracleChain.GetNodesFromCid(address.Bytes())
	isDel := false
	for _, v := range overlays {
		if s.overlay.Equal(v) {
			isDel = true
			break
		}
	}
	if !isDel {
		jsonhttp.Forbidden(w, fmt.Sprintf("address:%v Already Remove", address.String()))
	}

	hash, err := s.oracleChain.RemoveCidAndNode(r.Context(), address, s.overlay)
	if err != nil {
		s.logger.Error("aurora fileRegisterRemove failed: %v ", err)
		jsonhttp.InternalServerError(w, fmt.Sprintf("aurora fileRegisterRemove failed: %v ", err))
		return
	}

	err = s.fileInfo.RegisterFile(address, false)
	if err != nil {
		logger.Errorf("aurora fileRegister update info:%v", err)
	}

	trans := TransactionResponse{
		Hash:     hash,
		Address:  address,
		Register: false,
	}
	s.transactionChan <- trans

	jsonhttp.OK(w,
		auroraRegisterResponse{
			Hash: hash,
		})
}
