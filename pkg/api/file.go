package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/ethersphere/langos"
	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/collection/entry"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/joiner"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	multiPartFormData = "multipart/form-data"
)

// fileUploadResponse is returned when an HTTP request to upload a file is successful
type fileUploadResponse struct {
	Reference boson.Address `json:"reference"`
}
type fileListResponse struct {
	FileHash  string              `json:"fileHash"`
	Size      int                 `json:"size"`
	FileSize  int                 `json:"fileSize"`
	PinState  bool                `json:"pinState"`
	BitVector aurora.BitVectorApi `json:"bitVector"`
}

// fileUploadHandler uploads the file and its metadata supplied as:
// - multipart http message
// - other content types as complete file body
func (s *server) fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		reader                  io.Reader
		logger                  = tracing.NewLoggerWithTraceID(r.Context(), s.logger)
		fileName, contentLength string
		fileSize                uint64
		contentType             = r.Header.Get("Content-Type")
	)

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		logger.Debugf("file upload: parse content type header %q: %v", contentType, err)
		logger.Errorf("file upload: parse content type header %q", contentType)
		jsonhttp.BadRequest(w, "invalid content-type header")
		return
	}

	ctx := r.Context()

	if mediaType == multiPartFormData {
		mr := multipart.NewReader(r.Body, params["boundary"])

		// read only the first part, as only one file upload is supported
		part, err := mr.NextPart()
		if err != nil {
			logger.Debugf("file upload: read multipart: %v", err)
			logger.Error("file upload: read multipart")
			jsonhttp.BadRequest(w, "invalid multipart/form-data")
			return
		}

		// try to find filename
		// 1) in part header params
		// 2) as formname
		// 3) file reference hash (after uploading the file)
		if fileName = part.FileName(); fileName == "" {
			fileName = part.FormName()
		}

		// then find out content type
		contentType = part.Header.Get("Content-Type")
		if contentType == "" {
			br := bufio.NewReader(part)
			buf, err := br.Peek(512)
			if err != nil && err != io.EOF {
				logger.Debugf("file upload: read content type, file %q: %v", fileName, err)
				logger.Errorf("file upload: read content type, file %q", fileName)
				jsonhttp.BadRequest(w, "error reading content type")
				return
			}
			contentType = http.DetectContentType(buf)
			reader = br
		} else {
			reader = part
		}
		contentLength = part.Header.Get("Content-Length")
	} else {
		fileName = r.URL.Query().Get("name")
		contentLength = r.Header.Get("Content-Length")
		reader = r.Body
	}

	if contentLength != "" {
		fileSize, err = strconv.ParseUint(contentLength, 10, 64)
		if err != nil {
			logger.Debugf("file upload: content length, file %q: %v", fileName, err)
			logger.Errorf("file upload: content length, file %q", fileName)
			jsonhttp.BadRequest(w, "invalid content length header")
			return
		}
	} else {
		// copy the part to a tmp file to get its size
		tmp, err := ioutil.TempFile("", "aurorafs-multipart")
		if err != nil {
			logger.Debugf("file upload: create temporary file: %v", err)
			logger.Errorf("file upload: create temporary file")
			jsonhttp.InternalServerError(w, nil)
			return
		}
		defer os.Remove(tmp.Name())
		defer tmp.Close()
		n, err := io.Copy(tmp, reader)
		if err != nil {
			logger.Debugf("file upload: write temporary file: %v", err)
			logger.Error("file upload: write temporary file")
			jsonhttp.InternalServerError(w, nil)
			return
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			logger.Debugf("file upload: seek to beginning of temporary file: %v", err)
			logger.Error("file upload: seek to beginning of temporary file")
			jsonhttp.InternalServerError(w, nil)
			return
		}
		fileSize = uint64(n)
		reader = tmp
	}

	p := requestPipelineFn(s.storer, r)

	// first store the file and get its reference
	fr, err := p(ctx, reader, int64(fileSize))
	if err != nil {
		logger.Debugf("file upload: file store, file %q: %v", fileName, err)
		logger.Errorf("file upload: file store, file %q", fileName)
		jsonhttp.InternalServerError(w, "could not store file data")
		return
	}

	// If filename is still empty, use the file hash as the filename
	if fileName == "" {
		fileName = fr.String()
	}

	// then store the metadata and get its reference
	m := entry.NewMetadata(fileName)
	m.MimeType = contentType
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		logger.Debugf("file upload: metadata marshal, file %q: %v", fileName, err)
		logger.Errorf("file upload: metadata marshal, file %q", fileName)
		jsonhttp.InternalServerError(w, "metadata marshal error")
		return
	}

	mr, err := p(ctx, bytes.NewReader(metadataBytes), int64(len(metadataBytes)))
	if err != nil {
		logger.Debugf("file upload: metadata store, file %q: %v", fileName, err)
		logger.Errorf("file upload: metadata store, file %q", fileName)
		jsonhttp.InternalServerError(w, "could not store metadata")
		return
	}

	// now join both references (mr,fr) to create an entry and store it.
	entrie := entry.New(fr, mr)
	fileEntryBytes, err := entrie.MarshalBinary()
	if err != nil {
		logger.Debugf("file upload: entry marshal, file %q: %v", fileName, err)
		logger.Errorf("file upload: entry marshal, file %q", fileName)
		jsonhttp.InternalServerError(w, "entry marshal error")
		return
	}
	reference, err := p(ctx, bytes.NewReader(fileEntryBytes), int64(len(fileEntryBytes)))
	if err != nil {
		logger.Debugf("file upload: entry store, file %q: %v", fileName, err)
		logger.Errorf("file upload: entry store, file %q", fileName)
		jsonhttp.InternalServerError(w, "could not store entry")
		return
	}

	a, err := s.traversal.GetTrieData(ctx, reference)
	if err != nil {
		logger.Errorf("file upload: get trie data, file %q: %v", fileName, err)
		jsonhttp.InternalServerError(w, "could not get trie data")
		return
	}
	dataChunks, _ := s.traversal.CheckTrieData(ctx, reference, a)
	if err != nil {
		logger.Errorf("file upload: check trie data, file %q: %v", fileName, err)
		jsonhttp.InternalServerError(w, "check trie data error")
		return
	}
	for _, li := range dataChunks {
		for _, b := range li {
			s.chunkInfo.OnChunkTransferred(boson.NewAddress(b), reference, s.overlay, boson.ZeroAddress)
		}
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", reference.String()))

	jsonhttp.OK(w, fileUploadResponse{
		Reference: reference,
	})
}

// fileUploadInfo contains the data for a file to be uploaded
type fileUploadInfo struct {
	name        string // file name
	size        int64  // file size
	contentType string
	reader      io.Reader
}

// fileDownloadHandler downloads the file given the entry's reference.
func (s *server) fileDownloadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	nameOrHex := mux.Vars(r)["address"]

	address, err := s.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("file download: parse file address %s: %v", nameOrHex, err)
		logger.Errorf("file download: parse file address %s", nameOrHex)
		jsonhttp.NotFound(w, nil)
		return
	}

	r = r.WithContext(sctx.SetRootCID(r.Context(), address))
	if !s.chunkInfo.Init(r.Context(), nil, address) {
		logger.Debugf("file download: chunkInfo init %s: %v", nameOrHex, err)
		jsonhttp.NotFound(w, nil)
		return
	}

	// read entry
	j, _, err := joiner.New(r.Context(), s.storer, address)
	if err != nil {
		errors.Is(err, storage.ErrNotFound)
		logger.Debugf("file download: joiner %s: %v", address, err)
		logger.Errorf("file download: joiner %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	buf := bytes.NewBuffer(nil)
	_, err = file.JoinReadAll(r.Context(), j, buf)
	if err != nil {
		logger.Debugf("file download: read entry %s: %v", address, err)
		logger.Errorf("file download: read entry %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}
	e := &entry.Entry{}
	err = e.UnmarshalBinary(buf.Bytes())
	if err != nil {
		logger.Debugf("file download: unmarshal entry %s: %v", address, err)
		logger.Errorf("file download: unmarshal entry %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	// If none match header is set always send the reply as not modified
	// TODO: when SOC comes, we need to revisit this concept
	noneMatchEtag := r.Header.Get("If-None-Match")
	if noneMatchEtag != "" {
		if e.Reference().Equal(address) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// read metadata
	j, _, err = joiner.New(r.Context(), s.storer, e.Metadata())
	if err != nil {
		logger.Debugf("file download: joiner %s: %v", address, err)
		logger.Errorf("file download: joiner %s", address)
		jsonhttp.NotFound(w, nil)
		return
	}

	buf = bytes.NewBuffer(nil)
	_, err = file.JoinReadAll(r.Context(), j, buf)
	if err != nil {
		logger.Debugf("file download: read metadata %s: %v", nameOrHex, err)
		logger.Errorf("file download: read metadata %s", nameOrHex)
		jsonhttp.NotFound(w, nil)
		return
	}
	metaData := &entry.Metadata{}
	err = json.Unmarshal(buf.Bytes(), metaData)
	if err != nil {
		logger.Debugf("file download: unmarshal metadata %s: %v", nameOrHex, err)
		logger.Errorf("file download: unmarshal metadata %s", nameOrHex)
		jsonhttp.NotFound(w, nil)
		return
	}

	additionalHeaders := http.Header{
		"Content-Disposition": {fmt.Sprintf("inline; filename=\"%s\"", metaData.Filename)},
		"Content-Type":        {metaData.MimeType},
	}

	s.downloadHandler(w, r, e.Reference(), additionalHeaders, true)
}

// downloadHandler contains common logic for dowloading Aurora file from API
func (s *server) downloadHandler(w http.ResponseWriter, r *http.Request, reference boson.Address, additionalHeaders http.Header, etag bool) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)

	reader, l, err := joiner.New(r.Context(), s.storer, reference)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.chunkInfo.Init(r.Context(), nil, sctx.GetRootCID(r.Context()))
			logger.Debugf("api download: not found %s: %v", reference, err)
			logger.Error("api download: not found")
			jsonhttp.NotFound(w, nil)
			return
		}
		logger.Debugf("api download: invalid root chunk %s: %v", reference, err)
		logger.Error("api download: invalid root chunk")
		jsonhttp.NotFound(w, nil)
		return
	}

	// include additional headers
	for name, values := range additionalHeaders {
		var v string
		for _, value := range values {
			if v != "" {
				v += "; "
			}
			v += value
		}
		w.Header().Set(name, v)
	}
	if etag {
		w.Header().Set("ETag", fmt.Sprintf("%q", reference))
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Decompressed-Content-Length", fmt.Sprintf("%d", l))
	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")

	http.ServeContent(w, r, "", time.Now(), langos.NewBufferedLangos(reader, lookaheadBufferSize(l)))
}

func (s *server) fileDeleteHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("delete file: parse address: %v", err)
		s.logger.Errorf("delete file: parse address %s", addr)
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

		s.logger.Debugf("delete file: check %s exists: %v", hash, err)
		s.logger.Errorf("delete file: check %s exists", hash)
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
		s.logger.Errorf("delete file: chunk info report remove %s failed", hash)
		jsonhttp.InternalServerError(w, "file deleting occur error")
		return
	}

	for _, chunkHash := range chunkHashes {
		if chunkHash.Cid.Equal(hash) {
			continue
		}

		for i := 0; i < chunkHash.Number; i++ {
			err = s.storer.Set(r.Context(), storage.ModeSetRemove, chunkHash.Cid)
			if err != nil {
				if errors.Is(err, leveldb.ErrNotFound) {
					continue
				}

				s.logger.Debugf("delete file: remove chunk: %v", err)
				s.logger.Errorf("delete file: remove chunk %s", chunkHash.Cid)
				jsonhttp.InternalServerError(w, "file deletion occur error")
				return
			}
		}
	}

	err = s.storer.Set(r.Context(), storage.ModeSetRemove, hash)
	if err != nil {
		if !errors.Is(err, leveldb.ErrNotFound) {
			s.logger.Debugf("delete file: remove chunk: %v", err)
			s.logger.Errorf("delete file: remove chunk %s", hash)
			jsonhttp.InternalServerError(w, "file deletion occur error")
			return
		}
	}

	ok = s.chunkInfo.DelPyramid(hash)
	if !ok {
		s.logger.Errorf("delete file: chunk info report delete %s related pyramid failed", hash)
		jsonhttp.InternalServerError(w, "file deleting occur error")
		return
	}

	jsonhttp.OK(w, nil)
}

func (s *server) fileListHandler(w http.ResponseWriter, r *http.Request) {
	responseList := make([]fileListResponse, 0)

	fileListInfo, addressList := s.chunkInfo.GetFileList(s.overlay)

	if len(fileListInfo) > 0 && len(addressList) > 0 {
		yes, err := s.storer.HasMulti(context.Background(), storage.ModeHasPin, addressList...)
		if err != nil {
			return
		}
		for i, v := range addressList {
			//fileListInfo[v.String()].PinState = yes[i]
			Response := fileListResponse{}
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
