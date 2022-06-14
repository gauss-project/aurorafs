package chunkstore

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"strings"
)

type ChunkType int

const (
	DISCOVER ChunkType = iota
	SERVICE
	SOURCE
)

var TypeError = fmt.Errorf("type error")

type Interface interface {
	Init() error
	Put(chunkType ChunkType, reference boson.Address, providers []Provider) error
	Get(chunkType ChunkType, reference boson.Address) ([]Consumer, error)
	GetAll(chunkType ChunkType) (map[string][]Consumer, error)
	Remove(chunkType ChunkType, reference, overlay boson.Address) error
	RemoveAll(chunkType ChunkType, reference boson.Address) error
	Has(chunkType ChunkType, reference, overlay boson.Address) (bool, error)
}

type Provider struct {
	Overlay boson.Address
	Bit     int
	Len     int
	B       []byte
	Time    int64
}

type Consumer struct {
	Overlay boson.Address
	Len     int
	B       []byte
	Time    int64
}

type discoverBitVector struct {
	bit  *bitvector.BitVector
	time int64
}

type BitVector struct {
	Len int
	B   []byte
}

type chunkStore struct {
	stateStore storage.StateStorer
	source     map[string]map[string]*bitvector.BitVector
	service    map[string]map[string]*bitvector.BitVector
	discover   map[string]map[string]*discoverBitVector
}

func New(stateStore storage.StateStorer) Interface {
	return &chunkStore{
		stateStore: stateStore,
		source:     make(map[string]map[string]*bitvector.BitVector),
		service:    make(map[string]map[string]*bitvector.BitVector),
		discover:   make(map[string]map[string]*discoverBitVector),
	}
}

func (cs *chunkStore) Init() error {
	err := cs.initService()
	if err != nil {
		return err
	}
	err = cs.initDiscover()
	if err != nil {
		return err
	}
	err = cs.initSource()
	if err != nil {
		return err
	}
	return nil
}

func (cs *chunkStore) Put(chunkType ChunkType, reference boson.Address, providers []Provider) (err error) {

	switch chunkType {
	case DISCOVER:
		for _, provider := range providers {
			err = cs.putDiscover(reference, provider.Overlay, provider.B, provider.Len)
		}
	case SOURCE:
		for _, provider := range providers {
			err = cs.putChunkSource(reference, provider.Overlay, provider.Bit, provider.Len)
		}
	case SERVICE:
		for _, provider := range providers {
			err = cs.putChunkService(reference, provider.Overlay, provider.Bit, provider.Len)
		}
	default:
		return TypeError
	}
	return nil
}
func (cs *chunkStore) Get(chunkType ChunkType, reference boson.Address) ([]Consumer, error) {
	switch chunkType {
	case DISCOVER:
		d := cs.getDiscover(reference)
		p := make([]Consumer, 0, len(d))
		for k, v := range d {
			p = append(p, Consumer{
				Overlay: boson.MustParseHexAddress(k),
				Len:     v.bit.Len(),
				B:       v.bit.Bytes(),
				Time:    v.time,
			})
		}
		return p, nil
	case SOURCE:
		d := cs.getChunkSource(reference)
		p := make([]Consumer, 0, len(d))
		for k, v := range d {
			p = append(p, Consumer{
				Overlay: boson.MustParseHexAddress(k),
				Len:     v.Len(),
				B:       v.Bytes(),
			})
		}
		return p, nil
	case SERVICE:
		d := cs.getChunkService(reference)
		p := make([]Consumer, 0, len(d))
		for k, v := range d {
			p = append(p, Consumer{
				Overlay: boson.MustParseHexAddress(k),
				Len:     v.Len(),
				B:       v.Bytes(),
			})
		}
		return p, nil
	default:
		return nil, TypeError
	}
}

func (cs *chunkStore) GetAll(chunkType ChunkType) (map[string][]Consumer, error) {
	switch chunkType {
	case DISCOVER:
		r := make(map[string][]Consumer)
		d := cs.getAllDiscover()
		for rootCid, node := range d {
			p := make([]Consumer, 0, len(node))
			for overlay, bv := range node {
				p = append(p, Consumer{
					Overlay: boson.MustParseHexAddress(overlay),
					Len:     bv.bit.Len(),
					B:       bv.bit.Bytes(),
					Time:    bv.time,
				})
			}
			r[rootCid] = p
		}
		return r, nil
	case SOURCE:
		r := make(map[string][]Consumer)
		d := cs.getAllChunkSource()
		for rootCid, node := range d {
			p := make([]Consumer, 0, len(node))
			for overlay, bv := range node {
				p = append(p, Consumer{
					Overlay: boson.MustParseHexAddress(overlay),
					Len:     bv.Len(),
					B:       bv.Bytes(),
				})
			}
			r[rootCid] = p
		}
		return r, nil
	case SERVICE:
		r := make(map[string][]Consumer)
		d := cs.getAllChunkService()
		for rootCid, node := range d {
			p := make([]Consumer, 0, len(node))
			for overlay, bv := range node {
				p = append(p, Consumer{
					Overlay: boson.MustParseHexAddress(overlay),
					Len:     bv.Len(),
					B:       bv.Bytes(),
				})
			}
			r[rootCid] = p
		}
		return r, nil
	default:
		return nil, TypeError
	}
}

func (cs *chunkStore) Remove(chunkType ChunkType, reference, overlay boson.Address) error {
	switch chunkType {
	case DISCOVER:
		return cs.removeDiscoverByOverlay(reference, overlay)
	case SOURCE:
		return cs.removeSourceByOverlay(reference, overlay)
	case SERVICE:
		return cs.removeServiceByOverlay(reference, overlay)
	default:
		return TypeError
	}
}

func (cs *chunkStore) RemoveAll(chunkType ChunkType, reference boson.Address) error {
	switch chunkType {
	case DISCOVER:
		return cs.removeDiscover(reference)
	case SOURCE:
		return cs.removeSource(reference)
	case SERVICE:
		return cs.removeService(reference)
	default:
		return TypeError
	}
}

func (cs *chunkStore) Has(chunkType ChunkType, reference, overlay boson.Address) (bool, error) {
	switch chunkType {
	case DISCOVER:
		return cs.hasDiscover(reference, overlay), nil
	case SOURCE:
		return cs.hasSource(reference, overlay), nil
	case SERVICE:
		return cs.hasService(reference, overlay), nil
	default:
		return false, TypeError
	}
}

func generateKey(keyPrefix string, rootCid, overlay boson.Address) string {
	return keyPrefix + "-" + rootCid.String() + "-" + overlay.String()
}

func unmarshalKey(keyPrefix, key string) (boson.Address, boson.Address, error) {
	addr := strings.TrimPrefix(key, keyPrefix)
	keys := strings.Split(addr, "-")
	rootCid, err := boson.ParseHexAddress(keys[0])
	if err != nil {
		return boson.ZeroAddress, boson.ZeroAddress, err
	}
	overlay, err := boson.ParseHexAddress(keys[1])
	if err != nil {
		return boson.ZeroAddress, boson.ZeroAddress, err
	}
	return rootCid, overlay, nil
}
