// Package aurora exposes the data structure and operations
// necessary on the aurora.Address type which used in the handshake
// protocol, address-book and hive protocol.
package aurora

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrInvalidAddress = errors.New("invalid address")

// Address represents the aurora address in boson.
// It consists of a peers underlay (physical) address, overlay (topology) address and signature.
// Signature is used to verify the `Overlay/Underlay` pair, as it is based on `underlay|networkID`, signed with the public key of Overlay address
type Address struct {
	Underlay  ma.Multiaddr
	Overlay   boson.Address
	Signature []byte
}

type addressJSON struct {
	Overlay   string `json:"overlay"`
	Underlay  string `json:"underlay"`
	Signature string `json:"signature"`
}

type FileInfo struct {
	TreeSize  int          `json:"treeSize"`
	FileSize  int          `json:"fileSize"`
	Bitvector BitVectorApi `json:"bitvector"`
	PinState  bool         `json:"pinState"`
}

type ChunkInfoOverlay struct {
	Overlay string       `json:"overlay"`
	Bit     BitVectorApi `json:"bit"`
}

type BitVectorApi struct {
	Len int    `json:"len"`
	B   []byte `json:"b"`
}

type ChunkSourceApi struct {
	Overlay  string       `json:"overlay"`
	ChunkBit BitVectorApi `json:"chunkBit"`
}

type ChunkInfoSourceApi struct {
	PyramidSource string           `json:"pyramidSource"`
	ChunkSource   []ChunkSourceApi `json:"chunkSource"`
}

func NewAddress(signer crypto.Signer, underlay ma.Multiaddr, overlay boson.Address, networkID uint64) (*Address, error) {
	underlayBinary, err := underlay.MarshalBinary()
	if err != nil {
		return nil, err
	}

	signature, err := signer.Sign(generateSignData(underlayBinary, overlay.Bytes(), networkID))
	if err != nil {
		return nil, err
	}

	return &Address{
		Underlay:  underlay,
		Overlay:   overlay,
		Signature: signature,
	}, nil
}

func ParseAddress(underlay, overlay, signature []byte, networkID uint64) (*Address, error) {
	recoveredPK, err := crypto.Recover(signature, generateSignData(underlay, overlay, networkID))
	if err != nil {
		return nil, ErrInvalidAddress
	}

	recoveredOverlay, err := crypto.NewOverlayAddress(*recoveredPK, networkID)
	if err != nil {
		return nil, ErrInvalidAddress
	}
	if !bytes.Equal(recoveredOverlay.Bytes(), overlay) {
		return nil, ErrInvalidAddress
	}

	multiUnderlay, err := ma.NewMultiaddrBytes(underlay)
	if err != nil {
		return nil, ErrInvalidAddress
	}

	return &Address{
		Underlay:  multiUnderlay,
		Overlay:   boson.NewAddress(overlay),
		Signature: signature,
	}, nil
}

func generateSignData(underlay, overlay []byte, networkID uint64) []byte {
	networkIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(networkIDBytes, networkID)
	signData := append([]byte("aurorafs-handshake-"), underlay...)
	signData = append(signData, overlay...)
	return append(signData, networkIDBytes...)
}

func (a *Address) Equal(b *Address) bool {
	return a.Overlay.Equal(b.Overlay) && a.Underlay.Equal(b.Underlay) && bytes.Equal(a.Signature, b.Signature)
}

func (a *Address) MarshalJSON() ([]byte, error) {
	return json.Marshal(&addressJSON{
		Overlay:   a.Overlay.String(),
		Underlay:  a.Underlay.String(),
		Signature: base64.StdEncoding.EncodeToString(a.Signature),
	})
}

func (a *Address) UnmarshalJSON(b []byte) error {
	v := &addressJSON{}
	err := json.Unmarshal(b, v)
	if err != nil {
		return err
	}

	addr, err := boson.ParseHexAddress(v.Overlay)
	if err != nil {
		return err
	}

	a.Overlay = addr

	m, err := ma.NewMultiaddr(v.Underlay)
	if err != nil {
		return err
	}

	a.Underlay = m
	a.Signature, err = base64.StdEncoding.DecodeString(v.Signature)
	return err
}

func (a *Address) String() string {
	return fmt.Sprintf("[Underlay: %v, Overlay %v, Signature %x]", a.Underlay, a.Overlay, a.Signature)
}

// ShortString returns shortened versions of aurora address in a format: [Overlay, Underlay]
// It can be used for logging
func (a *Address) ShortString() string {
	return fmt.Sprintf("[Overlay: %s, Underlay: %s]", a.Overlay.String(), a.Underlay.String())
}
