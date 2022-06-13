package api

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/chunkinfo"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/sctx"
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"github.com/gorilla/mux"
)

// dirUploadHandler uploads a directory supplied as a tar in an HTTP request
func (s *server) dirUploadHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger)
	if r.Body == http.NoBody {
		logger.Error("aurora upload dir: request has no body")
		jsonhttp.BadRequest(w, invalidRequest)
		return
	}
	contentType := r.Header.Get(contentTypeHeader)
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		logger.Errorf("aurora upload dir: invalid content-type")
		logger.Debugf("aurora upload dir: invalid content-type err: %v", err)
		jsonhttp.BadRequest(w, invalidContentType)
		return
	}

	var dReader dirReader
	switch mediaType {
	case contentTypeTar:
		dReader = &tarReader{r: tar.NewReader(r.Body), logger: s.logger}
	case multiPartFormData:
		dReader = &multipartReader{r: multipart.NewReader(r.Body, params["boundary"])}
	default:
		logger.Error("aurora upload dir: invalid content-type for directory upload")
		jsonhttp.BadRequest(w, invalidContentType)
		return
	}
	defer r.Body.Close()

	ctx := r.Context()

	p := requestPipelineFn(s.storer, r)
	factory := requestPipelineFactory(ctx, s.storer, r)
	reference, err := storeDir(
		ctx,
		requestEncrypt(r),
		dReader,
		s.logger,
		p,
		loadsave.New(s.storer, factory),
		r.Header.Get(AuroraCollectionNameHeader),
		r.Header.Get(AuroraIndexDocumentHeader),
		r.Header.Get(AuroraErrorDocumentHeader),
	)
	if err != nil {
		logger.Debugf("aurora upload dir: store dir err: %v", err)
		logger.Errorf("aurora upload dir: store dir")
		jsonhttp.InternalServerError(w, directoryStoreError)
		return
	}

	dataChunks, _, err := s.traversal.GetChunkHashes(ctx, reference, nil)
	if err != nil {
		logger.Debugf("aurora upload dir: get chunk hashes err: %v", err)
		logger.Errorf("aurora upload dir: get chunk hashes err")
		jsonhttp.InternalServerError(w, "could not get chunk hashes")
		return
	}

	for _, li := range dataChunks {
		for _, b := range li {
			err := s.chunkInfo.OnChunkRetrieved(boson.NewAddress(b), reference, s.overlay)
			if err != nil {
				logger.Errorf("aurora upload dir: chunk transfer data err: %v", err)
				logger.Errorf("aurora upload dir: chunk transfer data err")
				jsonhttp.InternalServerError(w, "chunk transfer data error")
				return
			}
		}
	}

	if strings.ToLower(r.Header.Get(AuroraPinHeader)) == StringTrue {
		if err := s.pinning.CreatePin(r.Context(), reference, false); err != nil {
			logger.Debugf("aurora upload dir: creation of pin for %q failed: %v", reference, err)
			logger.Error("aurora upload dir: creation of pin failed")
			jsonhttp.InternalServerError(w, nil)
			return
		}
	}

	jsonhttp.Created(w, auroraUploadResponse{
		Reference: reference,
	})
}

// UnescapeUnicode convert the raw Unicode encoding to character
func UnescapeUnicode(raw string) (string, error) {
	str, err := strconv.Unquote(strings.Replace(strconv.Quote(raw), `\\u`, `\u`, -1))
	if err != nil {
		return "", err
	}
	return str, nil
}

// storeDir stores all files recursively contained in the directory given as a tar/multipart
// it returns the hash for the uploaded manifest corresponding to the uploaded dir
func storeDir(
	ctx context.Context,
	encrypt bool,
	reader dirReader,
	log logging.Logger,
	p pipelineFunc,
	ls file.LoadSaver,
	dirName,
	indexFilename,
	errorFilename string,
) (boson.Address, error) {
	logger := tracing.NewLoggerWithTraceID(ctx, log)

	dirManifest, err := manifest.NewDefaultManifest(ls, encrypt)
	if err != nil {
		return boson.ZeroAddress, err
	}

	if indexFilename != "" && strings.ContainsRune(indexFilename, '/') {
		return boson.ZeroAddress, fmt.Errorf("index document suffix must not include slash character")
	}

	if dirName != "" && strings.ContainsRune(dirName, '/') {
		return boson.ZeroAddress, fmt.Errorf("dir name must not include slash character")
	}

	filesAdded := 0

	// iterate through the files in the supplied tar
	for {
		fileInfo, err := reader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return boson.ZeroAddress, fmt.Errorf("read tar stream: %w", err)
		}

		fileReference, err := p(ctx, fileInfo.Reader)
		if err != nil {
			return boson.ZeroAddress, fmt.Errorf("store dir file: %w", err)
		}
		logger.Tracef("uploaded dir file %v with reference %v", fileInfo.Path, fileReference)

		fileMetadata := map[string]string{
			manifest.EntryMetadataContentTypeKey: fileInfo.ContentType,
			manifest.EntryMetadataFilenameKey:    fileInfo.Name,
		}
		// add file entry to dir manifest
		err = dirManifest.Add(ctx, fileInfo.Path, manifest.NewEntry(fileReference, fileMetadata))
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
	if dirName != "" || indexFilename != "" || errorFilename != "" {
		metadata := map[string]string{}
		if dirName != "" {
			realDirName, err := UnescapeUnicode(dirName)
			if err != nil {
				return boson.ZeroAddress, err
			}
			metadata[manifest.EntryMetadataDirnameKey] = realDirName
		}
		if indexFilename != "" {
			realIndexFilename, err := UnescapeUnicode(indexFilename)
			if err != nil {
				return boson.ZeroAddress, err
			}
			metadata[manifest.WebsiteIndexDocumentSuffixKey] = realIndexFilename
		}
		if errorFilename != "" {
			realErrorFilename, err := UnescapeUnicode(errorFilename)
			if err != nil {
				return boson.ZeroAddress, err
			}
			metadata[manifest.WebsiteErrorDocumentPathKey] = realErrorFilename
		}
		rootManifestEntry := manifest.NewEntry(boson.ZeroAddress, metadata)
		err = dirManifest.Add(ctx, manifest.RootPath, rootManifestEntry)
		if err != nil {
			return boson.ZeroAddress, fmt.Errorf("add to manifest: %w", err)
		}
	}

	var storeSizeFn []manifest.StoreSizeFunc

	// save manifest
	manifestReference, err := dirManifest.Store(ctx, storeSizeFn...)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("store manifest: %w", err)
	}
	logger.Tracef("finished uploaded dir with reference %v", manifestReference)

	return manifestReference, nil
}

type FileInfo struct {
	Path        string
	Name        string
	ContentType string
	Size        int64
	Reader      io.Reader
}

type dirReader interface {
	Next() (*FileInfo, error)
}

type tarReader struct {
	r      *tar.Reader
	logger logging.Logger
}

func (t *tarReader) Next() (*FileInfo, error) {
	for {
		fileHeader, err := t.r.Next()
		if err != nil {
			return nil, err
		}

		fileName := fileHeader.FileInfo().Name()
		contentType := mime.TypeByExtension(filepath.Ext(fileHeader.Name))
		fileSize := fileHeader.FileInfo().Size()
		filePath := filepath.Clean(fileHeader.Name)

		if filePath == "." {
			t.logger.Warning("skipping file upload empty path")
			continue
		}
		if runtime.GOOS == "windows" {
			// always use Unix path separator
			filePath = filepath.ToSlash(filePath)
		}
		// only store regular files
		if !fileHeader.FileInfo().Mode().IsRegular() {
			t.logger.Warningf("skipping file upload for %s as it is not a regular file", filePath)
			continue
		}

		return &FileInfo{
			Path:        filePath,
			Name:        fileName,
			ContentType: contentType,
			Size:        fileSize,
			Reader:      t.r,
		}, nil
	}
}

// multipart reader returns files added as a multipart form. We will ensure all the
// part headers are passed correctly
type multipartReader struct {
	r *multipart.Reader
}

func (m *multipartReader) Next() (*FileInfo, error) {
	part, err := m.r.NextPart()
	if err != nil {
		return nil, err
	}

	fileName := part.FileName()
	if fileName == "" {
		fileName = part.FormName()
	}
	if fileName == "" {
		return nil, errors.New("filename missing")
	}

	contentType := part.Header.Get(contentTypeHeader)
	if contentType == "" {
		return nil, errors.New("content-type missing")
	}

	contentLength := part.Header.Get("Content-Length")
	if contentLength == "" {
		return nil, errors.New("content-length missing")
	}
	fileSize, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, errors.New("invalid file size")
	}

	if filepath.Dir(fileName) != "." {
		return nil, errors.New("multipart upload supports only single directory")
	}

	return &FileInfo{
		Path:        fileName,
		Name:        fileName,
		ContentType: contentType,
		Size:        fileSize,
		Reader:      part,
	}, nil
}

func (s *server) auroraDeleteHandler(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	hash, err := boson.ParseHexAddress(addr)
	if err != nil {
		s.logger.Debugf("aurora delete: parse address: %w", err)
		s.logger.Errorf("aurora delete: parse address %s", addr)
		jsonhttp.BadRequest(w, "invalid address")
		return
	}

	r = r.WithContext(sctx.SetRootHash(r.Context(), hash))

	del := func() error {
		pyramid := s.chunkInfo.GetChunkPyramid(hash)
		chunkHashes := make([]chunkinfo.PyramidCidNum, len(pyramid))

		for i, chunk := range pyramid {
			chunkHashes[i] = *chunk
		}

		var err error

		for _, chunk := range chunkHashes {
			if chunk.Cid.Equal(hash) {
				continue
			}

			for i := 0; i < chunk.Number; i++ {
				err = s.storer.Set(r.Context(), storage.ModeSetRemove, chunk.Cid)
				if err != nil {
					if errors.Is(err, driver.ErrNotFound) {
						continue
					}

					return err
				}
			}
		}

		err = s.storer.Set(r.Context(), storage.ModeSetRemove, hash)
		if err != nil {
			if !errors.Is(err, driver.ErrNotFound) {
				return err
			}
		}

		return nil
	}
	err = s.chunkInfo.DelFile(hash, del)
	if err != nil {
		s.logger.Errorf("aurora delete: remove file: %w", err)
		jsonhttp.InternalServerError(w, "aurora deleting occur error")
		return
	}

	jsonhttp.OK(w, nil)
}
