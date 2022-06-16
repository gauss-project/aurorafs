package fileinfo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file/loadsave"
	"github.com/gauss-project/aurorafs/pkg/manifest"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"github.com/gauss-project/aurorafs/pkg/tracing"
	"strings"
)

var defaultPathVar = ""
var defaultDepth = 1

type ManifestNode struct {
	Type      string                   `json:"type"`
	Hash      string                   `json:"hash,omitempty"`
	Name      string                   `json:"name,omitempty"`
	Size      uint64                   `json:"size,omitempty"`
	Extension string                   `json:"ext,omitempty"`
	MimeType  string                   `json:"mime,omitempty"`
	Nodes     map[string]*ManifestNode `json:"sub,omitempty"`
}

var (
	ErrInvalidNameOrAddress = errors.New("invalid name or aurora address")
	ErrNoResolver           = errors.New("no resolver connected")
	ErrNotFound             = errors.New("manifest: not found")
	ErrServerError          = errors.New("manifest: ServerError")
)

func (f *FileInfo) ManifestView(ctx context.Context, nameOrHex string, pathVar string, depth int) (*ManifestNode, error) {
	logger := tracing.NewLoggerWithTraceID(ctx, f.logger)
	ls := loadsave.NewReadonly(f.localStore, storage.ModeGetRequest)

	address, err := f.resolveNameOrAddress(nameOrHex)
	if err != nil {
		logger.Debugf("manifest view: parse address %s: %v", nameOrHex, err)
		logger.Error("manifest view: parse address")
		return nil, ErrNotFound
	}

	// read manifest entry
	m, err := manifest.NewDefaultManifestReference(
		address,
		ls,
	)
	if err != nil {
		logger.Debugf("aurora download: not manifest %s: %v", address, err)
		logger.Errorf("aurora download: not manifest %s", address)
		return nil, ErrNotFound
	}

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

				refChunk, err := f.localStore.Get(ctx, storage.ModeGetRequest, boson.NewAddress(hash), 0)
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

		if err := m.IterateDirectories(ctx, []byte(pathVar), depth, fn); err != nil {
			return nil, ErrServerError
		}
	} else {
		e, err := m.Lookup(ctx, pathVar)
		if err != nil {
			logger.Debugf("manifest view: invalid filename: %v", err)
			logger.Error("manifest view: invalid filename")
			return nil, ErrNotFound
		}

		filename := e.Metadata()[manifest.EntryMetadataFilenameKey]
		extension := ""

		lastDot := strings.LastIndexByte(filename, '.')
		if lastDot != -1 {
			extension = filename[lastDot:]
		}

		refChunk, err := f.localStore.Get(ctx, storage.ModeGetRequest, e.Reference(), 0)
		if err != nil {
			logger.Debugf("manifest view: file not found: %v", err)
			logger.Error("manifest view: file not found")
			return nil, ErrNotFound
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
		_, err := m.Lookup(ctx, indexDocumentSuffixKey)
		if err != nil {
			logger.Debugf("manifest view: invalid index %s/%s: %v", address, indexDocumentSuffixKey, err)
			logger.Error("manifest view: invalid index")
		}

		indexNode, ok := rootNode.Nodes[indexDocumentSuffixKey]
		if ok && indexNode.Type == manifest.File.String() {
			indexNode.Type = manifest.IndexItem.String()
		}
	}

	return rootNode, nil
}

func manifestMetadataLoad(ctx context.Context, manifest manifest.Interface, path, metadataKey string) (string, bool) {
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

func (f *FileInfo) resolveNameOrAddress(str string) (boson.Address, error) {
	log := f.logger

	// Try and parse the name as a boson address.
	addr, err := boson.ParseHexAddress(str)
	if err == nil {
		log.Tracef("name resolve: valid aurora address %q", str)
		return addr, nil
	}

	// If no resolver is not available, return an error.
	if f.resolver == nil {
		return boson.ZeroAddress, ErrNoResolver
	}

	// Try and resolve the name using the provided resolver.
	log.Debugf("name resolve: attempting to resolve %s to aurora address", str)
	addr, err = f.resolver.Resolve(str)
	if err == nil {
		log.Tracef("name resolve: resolved name %s to %s", str, addr)
		return addr, nil
	}

	return boson.ZeroAddress, fmt.Errorf("%w: %v", ErrInvalidNameOrAddress, err)
}
