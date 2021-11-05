package manifest

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/file"
	"github.com/gauss-project/manifest/mantaray"
)

const (
	// ManifestMantarayContentType represents content type used for noting that
	// specific file should be processed as mantaray manifest.
	ManifestMantarayContentType = "application/aurora-manifest-mantaray+octet-stream"
)

type mantarayManifest struct {
	trie *mantaray.Node

	ls file.LoadSaver
}

// NewMantarayManifest creates a new mantaray-based manifest.
func NewMantarayManifest(
	ls file.LoadSaver,
	encrypted bool,
) (Interface, error) {
	mm := &mantarayManifest{
		trie: mantaray.New(),
		ls:   ls,
	}
	// use empty obfuscation key if not encrypting
	if !encrypted {
		// NOTE: it will be copied to all trie nodes
		mm.trie.SetObfuscationKey(mantaray.ZeroObfuscationKey)
	}
	return mm, nil
}

// NewMantarayManifestReference loads existing mantaray-based manifest.
func NewMantarayManifestReference(
	reference boson.Address,
	ls file.LoadSaver,
) (Interface, error) {
	return &mantarayManifest{
		trie: mantaray.NewNodeRef(reference.Bytes()),
		ls:   ls,
	}, nil
}

func (m *mantarayManifest) Type() string {
	return ManifestMantarayContentType
}

func (m *mantarayManifest) Add(ctx context.Context, path string, entry Entry) error {
	p := []byte(path)
	e := entry.Reference().Bytes()

	return m.trie.Add(ctx, p, e, entry.Metadata(), m.ls)
}

func (m *mantarayManifest) Remove(ctx context.Context, path string) error {
	p := []byte(path)

	err := m.trie.Remove(ctx, p, m.ls)
	if err != nil {
		if errors.Is(err, mantaray.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}

	return nil
}

func (m *mantarayManifest) Lookup(ctx context.Context, path string) (Entry, error) {
	p := []byte(path)

	node, err := m.trie.LookupNode(ctx, p, m.ls)
	if err != nil {
		if errors.Is(err, mantaray.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if !node.IsValueType() {
		return nil, ErrNotFound
	}

	address := boson.NewAddress(node.Entry())
	entry := NewEntry(address, node.Metadata())

	return entry, nil
}

func (m *mantarayManifest) HasPrefix(ctx context.Context, prefix string) (bool, error) {
	p := []byte(prefix)

	return m.trie.HasPrefix(ctx, p, m.ls)
}

func (m *mantarayManifest) Store(ctx context.Context, storeSizeFn ...StoreSizeFunc) (boson.Address, error) {
	var ls mantaray.LoadSaver
	if len(storeSizeFn) > 0 {
		ls = &mantarayLoadSaver{
			ls:          m.ls,
			storeSizeFn: storeSizeFn,
		}
	} else {
		ls = m.ls
	}

	err := m.trie.Save(ctx, ls)
	if err != nil {
		return boson.ZeroAddress, fmt.Errorf("manifest save error: %w", err)
	}

	address := boson.NewAddress(m.trie.Reference())

	return address, nil
}

func (m *mantarayManifest) IterateNodes(ctx context.Context, path []byte, level int, fn NodeIterFunc) error {
	reference := boson.NewAddress(m.trie.Reference())

	if boson.ZeroAddress.Equal(reference) {
		return ErrMissingReference
	}

	var uLevel uint
	if level > 0 && level < math.MaxInt32 {
		uLevel = uint(level)
	} else {
		uLevel = mantaray.MaxLevel
	}

	err := m.trie.WalkLevel(ctx, path, m.ls, uLevel, mantaray.WalkLevelFunc(fn))
	if err != nil {
		return fmt.Errorf("manifest walk level: %w", err)
	}

	return nil
}

func (m *mantarayManifest) IterateAddresses(ctx context.Context, fn boson.AddressIterFunc) error {
	reference := boson.NewAddress(m.trie.Reference())

	if boson.ZeroAddress.Equal(reference) {
		return ErrMissingReference
	}

	emptyAddr := boson.NewAddress([]byte{31: 0})
	walker := func(path []byte, node *mantaray.Node, err error) error {
		if err != nil {
			return err
		}

		if node != nil {
			if node.Reference() != nil {
				ref := boson.NewAddress(node.Reference())

				err = fn(ref)
				if err != nil {
					return err
				}
			}

			if node.IsValueType() && len(node.Entry()) > 0 {
				entry := boson.NewAddress(node.Entry())
				// The following comparison to the emptyAddr is
				// a dirty hack which prevents the walker to
				// fail when it encounters an empty address
				// (e.g.: during the unpin traversal operation
				// for manifest). This workaround should be
				// removed after the manifest serialization bug
				// is fixed.
				if entry.Equal(emptyAddr) {
					return nil
				}
				if err = fn(entry); err != nil {
					return err
				}
			}
		}

		return nil
	}

	err := m.trie.WalkNode(ctx, []byte{}, m.ls, walker)
	if err != nil {
		return fmt.Errorf("manifest iterate addresses: %w", err)
	}

	return nil
}

type mantarayLoadSaver struct {
	ls          file.LoadSaver
	storeSizeFn []StoreSizeFunc
}

func (ls *mantarayLoadSaver) Load(ctx context.Context, ref []byte) ([]byte, error) {
	return ls.ls.Load(ctx, ref)
}

func (ls *mantarayLoadSaver) Save(ctx context.Context, data []byte) ([]byte, error) {
	dataLen := int64(len(data))
	for i := range ls.storeSizeFn {
		err := ls.storeSizeFn[i](dataLen)
		if err != nil {
			return nil, fmt.Errorf("manifest store size func: %w", err)
		}
	}

	return ls.ls.Save(ctx, data)
}
