package api_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
	"strconv"
	"testing"

	"github.com/gauss-project/aurorafs/pkg/api"
	"github.com/gauss-project/aurorafs/pkg/boson"
	chunkinfo "github.com/gauss-project/aurorafs/pkg/chunkinfo/mock"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp/jsonhttptest"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	routetab "github.com/gauss-project/aurorafs/pkg/routetab/mock"
	"github.com/gauss-project/aurorafs/pkg/storage/mock"
	"github.com/gauss-project/aurorafs/pkg/traversal"
)

func TestDirs(t *testing.T) {
	var (
		auroraUploadResource   = "/aurora"
		auroraDownloadResource = func(addr, path string) string { return "/aurora/" + addr + "/" + path }
		ctx                    = context.Background()
		storer                 = mock.NewStorer()
		traverser              = traversal.New(storer)
		chunkinfo              = chunkinfo.New(routetab.NewMockRouteTable())
		logger                 = logging.New(io.Discard, 0)
		client, _, _           = newTestServer(t, testServerOptions{
			Storer:          storer,
			Logger:          logger,
			Traversal:       traverser,
			ChunkInfo:       chunkinfo,
			PreventRedirect: true,
		})
	)

	t.Run("empty request body", func(t *testing.T) {
		jsonhttptest.Request(t, client, http.MethodPost, auroraUploadResource, http.StatusBadRequest,
			jsonhttptest.WithRequestBody(bytes.NewReader(nil)),
			jsonhttptest.WithRequestHeader(api.AuroraCollectionHeader, "True"),
			jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
				Message: api.InvalidRequest.Error(),
				Code:    http.StatusBadRequest,
			}),
			jsonhttptest.WithRequestHeader("Content-Type", api.ContentTypeTar),
		)
	})

	t.Run("non tar file", func(t *testing.T) {
		file := bytes.NewReader([]byte("some data"))

		jsonhttptest.Request(t, client, http.MethodPost, auroraUploadResource, http.StatusInternalServerError,
			jsonhttptest.WithRequestBody(file),
			jsonhttptest.WithRequestHeader(api.AuroraCollectionHeader, "True"),
			jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
				Message: api.DirectoryStoreError.Error(),
				Code:    http.StatusInternalServerError,
			}),
			jsonhttptest.WithRequestHeader("Content-Type", api.ContentTypeTar),
		)
	})

	t.Run("wrong content type", func(t *testing.T) {
		tarReader := tarFiles(t, []f{{
			data: []byte("some data"),
			name: "binary-file",
		}})

		// submit valid tar, but with wrong content-type
		jsonhttptest.Request(t, client, http.MethodPost, auroraUploadResource, http.StatusBadRequest,
			jsonhttptest.WithRequestBody(tarReader),
			jsonhttptest.WithRequestHeader(api.AuroraCollectionHeader, "True"),
			jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
				Message: api.InvalidContentType.Error(),
				Code:    http.StatusBadRequest,
			}),
			jsonhttptest.WithRequestHeader("Content-Type", "other"),
		)
	})

	// valid tars
	for _, tc := range []struct {
		name                string
		expectedReference   boson.Address
		encrypt             bool
		wantIndexFilename   string
		wantErrorFilename   string
		indexFilenameOption jsonhttptest.Option
		errorFilenameOption jsonhttptest.Option
		doMultipart         bool
		files               []f // files in dir for test case
	}{
		{
			name:              "non-nested files without extension",
			expectedReference: boson.MustParseHexAddress("a0483def3b494876b095bc3c309a87c4f736a9c4610aae7a4b188eedff179fc7"),
			files: []f{
				{
					data: []byte("first file data"),
					name: "file1",
					dir:  "",
					header: http.Header{
						"Content-Type": {""},
					},
				},
				{
					data: []byte("second file data"),
					name: "file2",
					dir:  "",
					header: http.Header{
						"Content-Type": {""},
					},
				},
			},
		},
		{
			name:              "nested files with extension",
			expectedReference: boson.MustParseHexAddress("f643b3241ab3d8d217ca83c26198e2543773b61264a3efd74186f95ace4823de"),
			files: []f{
				{
					data: []byte("robots text"),
					name: "robots.txt",
					dir:  "",
					header: http.Header{
						"Content-Type": {"text/plain; charset=utf-8"},
					},
				},
				{
					data: []byte("image 1"),
					name: "1.png",
					dir:  "img",
					header: http.Header{
						"Content-Type": {"image/png"},
					},
				},
				{
					data: []byte("image 2"),
					name: "2.png",
					dir:  "img",
					header: http.Header{
						"Content-Type": {"image/png"},
					},
				},
			},
		},
		{
			name:              "no index filename",
			expectedReference: boson.MustParseHexAddress("d501c73d3027f9abeb52f43436231beb8f94e07c783b93f540f67221cf5bf767"),
			files: []f{
				{
					data: []byte("<h1>Aurora"),
					name: "index.html",
					dir:  "",
					header: http.Header{
						"Content-Type": {"text/html; charset=utf-8"},
					},
				},
			},
		},
		{
			name:                "explicit index filename",
			expectedReference:   boson.MustParseHexAddress("d053459b361fe7a467c0d114ca1c549596e30538b7e3a01974e9e22940755130"),
			wantIndexFilename:   "index.html",
			indexFilenameOption: jsonhttptest.WithRequestHeader(api.AuroraIndexDocumentHeader, "index.html"),
			files: []f{
				{
					data: []byte("<h1>Aurora"),
					name: "index.html",
					dir:  "",
					header: http.Header{
						"Content-Type": {"text/html; charset=utf-8"},
					},
				},
			},
		},
		{
			name:                "nested index filename",
			expectedReference:   boson.MustParseHexAddress("704eebd4073ad481de0245a39cf574cff06fc1516ec98a85a50a584a35790a4c"),
			wantIndexFilename:   "index.html",
			indexFilenameOption: jsonhttptest.WithRequestHeader(api.AuroraIndexDocumentHeader, "index.html"),
			files: []f{
				{
					data: []byte("<h1>Aurora"),
					name: "index.html",
					dir:  "dir",
					header: http.Header{
						"Content-Type": {"text/html; charset=utf-8"},
					},
				},
			},
		},
		{
			name:                "explicit index and error filename",
			expectedReference:   boson.MustParseHexAddress("f1bd48e173b8f5582e533686a307c82c2e788eb73e6bc7b2f04d46e4123056e5"),
			wantIndexFilename:   "index.html",
			wantErrorFilename:   "error.html",
			indexFilenameOption: jsonhttptest.WithRequestHeader(api.AuroraIndexDocumentHeader, "index.html"),
			errorFilenameOption: jsonhttptest.WithRequestHeader(api.AuroraErrorDocumentHeader, "error.html"),
			doMultipart:         true,
			files: []f{
				{
					data: []byte("<h1>Aurora"),
					name: "index.html",
					dir:  "",
					header: http.Header{
						"Content-Type": {"text/html; charset=utf-8"},
					},
				},
				{
					data: []byte("<h2>404"),
					name: "error.html",
					dir:  "",
					header: http.Header{
						"Content-Type": {"text/html; charset=utf-8"},
					},
				},
			},
		},
		{
			name:              "invalid archive paths",
			expectedReference: boson.MustParseHexAddress("63c6ccb2a2eaf563cf43c56afd86f12edfa10d41f3b824845cbe962914d1e618"),
			files: []f{
				{
					data:     []byte("<h1>Aurora"),
					name:     "index.html",
					dir:      "",
					filePath: "./index.html",
				},
				{
					data:     []byte("body {}"),
					name:     "app.css",
					dir:      "",
					filePath: "./app.css",
				},
				{
					data: []byte(`User-agent: *
Disallow: /`),
					name:     "robots.txt",
					dir:      "",
					filePath: "./robots.txt",
				},
			},
		},
		{
			name:    "encrypted",
			encrypt: true,
			files: []f{
				{
					data:     []byte("<h1>Aurora"),
					name:     "index.html",
					dir:      "",
					filePath: "./index.html",
				},
			},
		},
	} {
		verify := func(t *testing.T, resp api.AuroraUploadResponse) {
			t.Helper()
			// NOTE: reference will be different each time when encryption is enabled
			if !tc.encrypt {
				if !resp.Reference.Equal(tc.expectedReference) {
					t.Fatalf("expected root reference to match %s, got %s", tc.expectedReference, resp.Reference)
				}
			}

			// verify manifest content
			verifyManifest, err := manifest.NewDefaultManifestReference(
				resp.Reference,
				loadsave.NewReadonly(storer),
			)
			if err != nil {
				t.Fatal(err)
			}

			validateFile := func(t *testing.T, file f, filePath string) {
				t.Helper()

				jsonhttptest.Request(t, client, http.MethodGet,
					auroraDownloadResource(resp.Reference.String(), filePath),
					http.StatusOK,
					jsonhttptest.WithExpectedResponse(file.data),
					jsonhttptest.WithRequestHeader("Content-Type", file.header.Get("Content-Type")),
				)
			}

			validateIsPermanentRedirect := func(t *testing.T, fromPath, toPath string) {
				t.Helper()

				expectedResponse := fmt.Sprintf("<a href=\"%s\">Permanent Redirect</a>.\n\n",
					auroraDownloadResource(resp.Reference.String(), toPath))

				jsonhttptest.Request(t, client, http.MethodGet,
					auroraDownloadResource(resp.Reference.String(), fromPath),
					http.StatusPermanentRedirect,
					jsonhttptest.WithExpectedResponse([]byte(expectedResponse)),
				)
			}

			validateAltPath := func(t *testing.T, fromPath, toPath string) {
				t.Helper()

				var respBytes []byte

				jsonhttptest.Request(t, client, http.MethodGet,
					auroraDownloadResource(resp.Reference.String(), toPath), http.StatusOK,
					jsonhttptest.WithPutResponseBody(&respBytes),
				)

				jsonhttptest.Request(t, client, http.MethodGet,
					auroraDownloadResource(resp.Reference.String(), fromPath), http.StatusOK,
					jsonhttptest.WithExpectedResponse(respBytes),
				)
			}

			// check if each file can be located and read
			for _, file := range tc.files {
				validateFile(t, file, path.Join(file.dir, file.name))
			}

			// check index filename
			if tc.wantIndexFilename != "" {
				entry, err := verifyManifest.Lookup(ctx, manifest.RootPath)
				if err != nil {
					t.Fatal(err)
				}

				manifestRootMetadata := entry.Metadata()
				indexDocumentSuffixPath, ok := manifestRootMetadata[manifest.WebsiteIndexDocumentSuffixKey]
				if !ok {
					t.Fatalf("expected index filename '%s', did not find any", tc.wantIndexFilename)
				}

				// check index suffix for each dir
				for _, file := range tc.files {
					if file.dir != "" {
						validateIsPermanentRedirect(t, file.dir, file.dir+"/")
						validateAltPath(t, file.dir+"/", path.Join(file.dir, indexDocumentSuffixPath))
					}
				}
			}

			// check error filename
			if tc.wantErrorFilename != "" {
				entry, err := verifyManifest.Lookup(ctx, manifest.RootPath)
				if err != nil {
					t.Fatal(err)
				}

				manifestRootMetadata := entry.Metadata()
				errorDocumentPath, ok := manifestRootMetadata[manifest.WebsiteErrorDocumentPathKey]
				if !ok {
					t.Fatalf("expected error filename '%s', did not find any", tc.wantErrorFilename)
				}

				// check error document
				validateAltPath(t, "_non_existent_file_path_", errorDocumentPath)
			}

		}
		t.Run(tc.name, func(t *testing.T) {
			// tar all the test case files
			tarReader := tarFiles(t, tc.files)

			var resp api.AuroraUploadResponse

			options := []jsonhttptest.Option{
				jsonhttptest.WithRequestBody(tarReader),
				jsonhttptest.WithRequestHeader(api.AuroraCollectionHeader, "True"),
				jsonhttptest.WithRequestHeader("Content-Type", api.ContentTypeTar),
				jsonhttptest.WithUnmarshalJSONResponse(&resp),
			}
			if tc.indexFilenameOption != nil {
				options = append(options, tc.indexFilenameOption)
			}
			if tc.errorFilenameOption != nil {
				options = append(options, tc.errorFilenameOption)
			}
			if tc.encrypt {
				options = append(options, jsonhttptest.WithRequestHeader(api.AuroraEncryptHeader, "true"))
			}

			// verify directory tar upload response
			jsonhttptest.Request(t, client, http.MethodPost, auroraUploadResource, http.StatusCreated, options...)

			if resp.Reference.String() == "" {
				t.Fatalf("expected file reference, did not got any")
			}

			verify(t, resp)
		})
		if tc.doMultipart {
			t.Run("multipart_upload", func(t *testing.T) {
				// tar all the test case files
				mwReader, mwBoundary := multipartFiles(t, tc.files)

				var resp api.AuroraUploadResponse

				options := []jsonhttptest.Option{
					jsonhttptest.WithRequestBody(mwReader),
					jsonhttptest.WithRequestHeader(api.AuroraCollectionHeader, "True"),
					jsonhttptest.WithRequestHeader("Content-Type", fmt.Sprintf("multipart/form-data; boundary=%q", mwBoundary)),
					jsonhttptest.WithUnmarshalJSONResponse(&resp),
				}
				if tc.indexFilenameOption != nil {
					options = append(options, tc.indexFilenameOption)
				}
				if tc.errorFilenameOption != nil {
					options = append(options, tc.errorFilenameOption)
				}
				if tc.encrypt {
					options = append(options, jsonhttptest.WithRequestHeader(api.AuroraEncryptHeader, "true"))
				}

				// verify directory tar upload response
				jsonhttptest.Request(t, client, http.MethodPost, auroraUploadResource, http.StatusCreated, options...)

				if resp.Reference.String() == "" {
					t.Fatalf("expected file reference, did not got any")
				}

				verify(t, resp)
			})
		}
	}
}

// tarFiles receives an array of test case files and creates a new tar with those files as a collection
// it returns a bytes.Buffer which can be used to read the created tar
func tarFiles(t *testing.T, files []f) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, file := range files {
		filePath := path.Join(file.dir, file.name)
		if file.filePath != "" {
			filePath = file.filePath
		}

		// create tar header and write it
		hdr := &tar.Header{
			Name: filePath,
			Mode: 0600,
			Size: int64(len(file.data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}

		// write the file data to the tar
		if _, err := tw.Write(file.data); err != nil {
			t.Fatal(err)
		}
	}

	// finally close the tar writer
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	return &buf
}

func multipartFiles(t *testing.T, files []f) (*bytes.Buffer, string) {
	t.Helper()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	for _, file := range files {
		hdr := make(textproto.MIMEHeader)
		if file.name != "" {
			hdr.Set("Content-Disposition", fmt.Sprintf("form-data; name=%q", file.name))

		}
		contentType := file.header.Get("Content-Type")
		if contentType != "" {
			hdr.Set("Content-Type", contentType)

		}
		if len(file.data) > 0 {
			hdr.Set("Content-Length", strconv.Itoa(len(file.data)))

		}
		part, err := mw.CreatePart(hdr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.Copy(part, bytes.NewBuffer(file.data)); err != nil {
			t.Fatal(err)
		}
	}

	// finally close the tar writer
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}

	return &buf, mw.Boundary()
}

// struct for dir files for test cases
type f struct {
	data     []byte
	name     string
	dir      string
	filePath string
	header   http.Header
}
