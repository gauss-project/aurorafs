package chunkstore

import (
	"github.com/gauss-project/aurorafs/pkg/bitvector"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"time"
)

var discoverKeyPrefix = "discover"

func (cs *chunkStore) putChunkDiscover(rootCid, overlay boson.Address, b []byte, len int) error {
	r := rootCid.String()
	o := overlay.String()
	v, ok := cs.discover[r]
	if !ok {
		v = make(map[string]*discoverBitVector)
	}
	var data = &discoverBitVector{}

	if data, ok = cs.discover[r][o]; ok {
		if data.bit.Len() < len {
			bv, err := bitvector.NewFromBytes(b, len)
			if err != nil {
				return err
			}
			err = bv.SetBytes(data.bit.Bytes())
			if err != nil {
				return err
			}
			data.bit = bv
		} else {
			err := data.bit.SetBytes(b)
			if err != nil {
				return err
			}
		}
	} else {
		bv, err := bitvector.NewFromBytes(b, len)
		if err != nil {
			return err
		}
		data = &discoverBitVector{
			bit:  bv,
			time: time.Now().Unix(),
		}
	}
	v[o] = data
	cs.discover[r] = v
	// cs
	if err := cs.stateStore.Put(generateKey(discoverKeyPrefix, rootCid, overlay),
		BitVector{B: data.bit.Bytes(), Len: data.bit.Len()}); err != nil {
		return err
	}
	return nil
}

func (cs *chunkStore) getChunkDiscover(rootCid boson.Address) map[string]*discoverBitVector {
	r := rootCid.String()
	return cs.discover[r]
}

func (cs *chunkStore) removeDiscover(rootCid boson.Address) error {
	r := rootCid.String()
	if v, ok := cs.discover[r]; ok {
		for k := range v {
			err := cs.stateStore.Delete(generateKey(discoverKeyPrefix, rootCid, boson.MustParseHexAddress(k)))
			if err != nil {
				return err
			}
		}
		delete(cs.discover, r)
	}

	return nil
}

func (cs *chunkStore) removeDiscoverByOverlay(rootCid, overlay boson.Address) error {
	r := rootCid.String()
	o := overlay.String()
	if _, ok := cs.discover[r][o]; ok {
		err := cs.stateStore.Delete(generateKey(discoverKeyPrefix, rootCid, overlay))
		if err != nil {
			return err
		}
		delete(cs.discover[r], o)
	}
	return nil
}

func (cs *chunkStore) hasDiscover(rootCid, overlay boson.Address) bool {
	r := rootCid.String()
	o := overlay.String()
	_, ok := cs.discover[r][o]
	return ok
}
