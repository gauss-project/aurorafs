package debugapi

import (
	"bytes"
	"encoding/hex"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/keystore/file"
	"io"
	"net/http"
	"path/filepath"
)

func (s *Service) importKeyHandler(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.nodeOptions.DataDir, "keys")
	f := file.New(path)
	password := r.FormValue("password")
	keyJson := r.FormValue("keystore")
	pkData := r.FormValue("private_key")
	if pkData == "" && keyJson == "" {
		jsonhttp.InternalServerError(w, "Please enter the private_key or keystore")
		return
	}
	if keyJson != "" {
		err := f.ImportKey("boson", password, []byte(keyJson))
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
	} else {
		b, err := hex.DecodeString(pkData)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		pk, err := crypto.DecodeSecp256k1PrivateKey(b)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		err = f.ImportPrivateKey("boson", password, pk)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
	}

	jsonhttp.OK(w, nil)
}

func (s *Service) exportKeyHandler(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.nodeOptions.DataDir, "keys")
	f := file.New(path)
	password := r.FormValue("password")
	tp := r.FormValue("type")
	if tp == "private" {
		privateKey, _, err := f.Key("boson", password)
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		type out struct {
			PrivateKey string `json:"private_key"`
		}
		pk := crypto.EncodeSecp256k1PrivateKey(privateKey)
		jsonhttp.OK(w, out{PrivateKey: hex.EncodeToString(pk)})
		return
	}
	b, err := f.ExportKey("boson", password)
	if err != nil {
		jsonhttp.InternalServerError(w, err)
		return
	}
	w.Header().Set("Content-Type", jsonhttp.DefaultContentTypeHeader)
	_, _ = io.Copy(w, bytes.NewBuffer(b))
}
