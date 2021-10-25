package api

import "github.com/gauss-project/aurorafs/pkg/boson"

type Server = server

type (
	BytesPostResponse    = bytesPostResponse
	ChunkAddressResponse = chunkAddressResponse
	SocPostResponse      = socPostResponse
	AuroraUploadResponse = auroraUploadResponse
)

var (
	InvalidContentType  = invalidContentType
	InvalidRequest      = invalidRequest
	DirectoryStoreError = directoryStoreError
)

var (
	ContentTypeTar = contentTypeTar
)

var (
	ErrNoResolver           = errNoResolver
	ErrInvalidNameOrAddress = errInvalidNameOrAddress
)

func (s *Server) ResolveNameOrAddress(str string) (boson.Address, error) {
	return s.resolveNameOrAddress(str)
}

func CalculateNumberOfChunks(contentLength int64, isEncrypted bool) int64 {
	return calculateNumberOfChunks(contentLength, isEncrypted)
}
