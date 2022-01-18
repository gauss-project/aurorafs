package debugapi

import (
	"bytes"
	"github.com/gauss-project/aurorafs/pkg/keystore/file"
	"io"
	"net/http"
	"path/filepath"

	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
)

func (s *Service) privateKeyHandler(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.nodeOptions.DataDir, "keys")
	f := file.New(path)

	if r.Method == "POST" {
		// import
		keyJson := r.FormValue("key")
		password := r.FormValue("pwd")

		err := f.ImportKey("boson", password, []byte(keyJson))
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}
		jsonhttp.OK(w, nil)
	} else {
		// export get
		b, err := f.ExportKey("boson")
		if err != nil {
			jsonhttp.InternalServerError(w, err)
			return
		}

		w.Header().Set("Content-Type", jsonhttp.DefaultContentTypeHeader)
		_, _ = io.Copy(w, bytes.NewBuffer(b))
	}
}
