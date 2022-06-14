package chunkstore

import (
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"strings"
)

var keyPrefix = "chunk"

func (cs *chunkStore) initService() error {
	if err := cs.stateStore.Iterate(keyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), keyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(keyPrefix, key)
		if err != nil {
			return false, err
		}
		var vb BitVector
		if err := cs.stateStore.Get(key, &vb); err != nil {
			return false, err
		}
		cs.putInitService(rootCid, overlay, vb.B, vb.Len)
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func (cs *chunkStore) getChunkService(rootCid boson.Address) map[string]*bitvector.BitVector {
	r := rootCid.String()
	return cs.service[r]
}

func (cs *chunkStore) getAllChunkService() map[string]map[string]*bitvector.BitVector {
	return cs.service
}

func (cs *chunkStore) putInitService(rootCid, overlay boson.Address, b []byte, len int) {
	if cs.hasDiscover(rootCid, overlay) {
		return
	}
	r := rootCid.String()
	o := overlay.String()
	v, ok := cs.service[r]
	if !ok {
		v = make(map[string]*bitvector.BitVector)
	}
	bv, err := bitvector.NewFromBytes(b, len)
	if err != nil {
		return
	}
	v[o] = bv
	cs.service[r] = v
}

func (cs *chunkStore) putChunkService(rootCid, overlay boson.Address, bit, len int) error {
	r := rootCid.String()
	o := overlay.String()
	v, ok := cs.service[r]
	if !ok {
		v = make(map[string]*bitvector.BitVector)
	}
	bv, ok := v[o]
	if ok {
		if bv.Len() < len {
			newbv, err := bitvector.New(len)
			if err != nil {
				return err
			}
			err = newbv.SetBytes(bv.Bytes())
			if err != nil {
				return err
			}
			bv = newbv
		}
	} else {
		newbv, err := bitvector.New(len)
		if err != nil {
			return err
		}
		v[o] = newbv
		cs.service[r] = v
		bv = newbv
	}
	bv.Set(bit)
	return cs.stateStore.Put(generateKey(keyPrefix, rootCid, overlay), &BitVector{B: bv.Bytes(), Len: bv.Len()})
}

func (cs *chunkStore) removeService(rootCid boson.Address) error {
	r := rootCid.String()
	if v, ok := cs.service[r]; ok {
		for k := range v {
			err := cs.stateStore.Delete(generateKey(keyPrefix, rootCid, boson.MustParseHexAddress(k)))
			if err != nil {
				return err
			}
		}
		delete(cs.service, r)
	}

	return nil
}

func (cs *chunkStore) removeServiceByOverlay(rootCid, overlay boson.Address) error {
	r := rootCid.String()
	o := overlay.String()
	if _, ok := cs.service[r][o]; ok {
		err := cs.stateStore.Delete(generateKey(keyPrefix, rootCid, overlay))
		if err != nil {
			return err
		}
		delete(cs.service[r], o)
	}
	return nil
}

func (cs *chunkStore) hasService(rootCid, overlay boson.Address) bool {
	r := rootCid.String()
	o := overlay.String()
	_, ok := cs.service[r][o]
	return ok
}
