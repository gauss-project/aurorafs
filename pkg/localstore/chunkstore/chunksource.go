package chunkstore

import (
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"strings"
)

var sourceKeyPrefix = "source"

func (cs *chunkStore) initSource() error {
	if err := cs.stateStore.Iterate(sourceKeyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), sourceKeyPrefix) {
			return true, nil
		}
		key := string(k)
		rootCid, overlay, err := unmarshalKey(sourceKeyPrefix, key)
		if err != nil {
			return false, err
		}
		var vb BitVector
		if err := cs.stateStore.Get(key, &vb); err != nil {
			return false, err
		}
		cs.putInitSource(rootCid, overlay, vb.B, vb.Len)
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}
func (cs *chunkStore) getChunkSource(rootCid boson.Address) map[string]*bitvector.BitVector {
	r := rootCid.String()
	return cs.source[r]
}

func (cs *chunkStore) getAllChunkSource() map[string]map[string]*bitvector.BitVector {
	return cs.source
}

func (cs *chunkStore) putInitSource(rootCid, overlay boson.Address, b []byte, len int) {
	if cs.hasDiscover(rootCid, overlay) {
		return
	}
	r := rootCid.String()
	o := overlay.String()
	v, ok := cs.source[r]
	if !ok {
		v = make(map[string]*bitvector.BitVector)
	}
	bv, err := bitvector.NewFromBytes(b, len)
	if err != nil {
		return
	}
	v[o] = bv
	cs.source[r] = v
}

func (cs *chunkStore) putChunkSource(rootCid, source boson.Address, bit, len int) error {
	r := rootCid.String()
	o := source.String()
	v, ok := cs.source[r]
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
		cs.source[r] = v
		bv = newbv
	}
	bv.Set(bit)
	return cs.stateStore.Put(generateKey(sourceKeyPrefix, rootCid, source), &BitVector{B: bv.Bytes(), Len: bv.Len()})
}

func (cs *chunkStore) removeSource(rootCid boson.Address) error {
	r := rootCid.String()
	if v, ok := cs.source[r]; ok {
		for k := range v {
			err := cs.stateStore.Delete(generateKey(sourceKeyPrefix, rootCid, boson.MustParseHexAddress(k)))
			if err != nil {
				return err
			}
		}
		delete(cs.source, r)
	}
	return nil
}

func (cs *chunkStore) removeSourceByOverlay(rootCid, overlay boson.Address) error {
	r := rootCid.String()
	o := overlay.String()
	if _, ok := cs.source[r][o]; ok {
		err := cs.stateStore.Delete(generateKey(sourceKeyPrefix, rootCid, overlay))
		if err != nil {
			return err
		}
		delete(cs.source[r], o)
	}
	return nil
}

func (cs *chunkStore) hasSource(rootCid, overlay boson.Address) bool {
	r := rootCid.String()
	o := overlay.String()
	_, ok := cs.source[r][o]
	return ok
}
