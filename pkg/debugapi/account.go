package debugapi

import (
	"fmt"
	"net/http"

	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

func (s *Service) privateKeyHandler(w http.ResponseWriter, r *http.Request) {
	pk := crypto.EncodeSecp256k1PrivateKey(s.nodeOptions.PrivateKey)

	type out struct {
		PrivateKey string `json:"private_key"`
	}
	jsonhttp.OK(w, out{PrivateKey: fmt.Sprintf("%x", pk)})
}
