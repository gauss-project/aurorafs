package cert

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func GenerateCert(dir string) (certFile string, keyFile string) {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, max)

	subject := pkix.Name{
		Country:            []string{"AU"},
		Province:           []string{"Some-State"},
		Organization:       []string{"Aurora File System"},
		OrganizationalUnit: []string{},
	}
	rootTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(3650 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	pk, _ := rsa.GenerateKey(rand.Reader, 2048)
	derBytes, _ := x509.CreateCertificate(rand.Reader, &rootTemplate, &rootTemplate, &pk.PublicKey, pk)

	_, err := os.Stat(dir + "/cert")
	if err != nil {
		if os.IsNotExist(err) {
			_ = os.Mkdir(dir+"/cert", 0744)
		}
	}
	certFile = filepath.Join(dir, "cert", "cert.pem")
	certOut, _ := os.OpenFile(certFile, os.O_WRONLY|os.O_CREATE, 0744)
	_ = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	_ = certOut.Close()

	keyFile = filepath.Join(dir, "cert", "key.pem")
	keyOut, _ := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE, 0744)
	_ = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(pk)})
	_ = keyOut.Close()
	return
}
