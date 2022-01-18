package debugapi

import (
	"bytes"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/gauss-project/aurorafs/pkg/keystore/file"
	"github.com/google/uuid"
	"io"
	"net/http"
	"path/filepath"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

func (s *Service) privateKeyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		// import
		keyJson := r.FormValue("key")
		password := r.FormValue("pwd")
		key, err := keystore.DecryptKey([]byte(keyJson), password)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		path := filepath.Join(s.nodeOptions.DataDir, "keys")
		f := file.New(path)
		err = f.BackKey("boson")
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		err = f.ImportKey("boson", password, key.PrivateKey)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		jsonhttp.OK(w, nil)
	} else {
		// export get
		if s.nodeOptions.Signer == nil {
			jsonhttp.NotFound(w, "no signer")
			return
		}
		ethAddr, err := s.nodeOptions.Signer.EthereumAddress()
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		b, err := keystore.EncryptKey(&keystore.Key{
			Id:         uuid.New(),
			Address:    ethAddr,
			PrivateKey: s.nodeOptions.Signer.PrivateKey(),
		}, s.nodeOptions.Password, keystore.LightScryptN, keystore.LightScryptP)

		w.Header().Set("Content-Type", jsonhttp.DefaultContentTypeHeader)
		_, _ = io.Copy(w, bytes.NewBuffer(b))
	}
}
