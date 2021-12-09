package debugapi

import "net/http"

func (s *Service) privateKeyHandler(w http.ResponseWriter, r *http.Request) {
	s.nodeOptions.PrivateKey.D.Bytes()
}