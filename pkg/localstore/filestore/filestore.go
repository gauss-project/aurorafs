package filestore

import (
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/storage"
	"strings"
)

type Interface interface {
	Init() error
	Get(reference boson.Address) (FileView, bool)
	GetList(page Page, filter []Filter, sort Sort) []FileView
	Put(file FileView) error
	Delete(reference boson.Address) error
	Has(reference boson.Address) bool
	Update(file FileView) error
}
type fileStore struct {
	stateStore storage.StateStorer
	files      map[string]FileView
}

type FileView struct {
	RootCid    boson.Address
	Hash       string
	Pinned     bool
	Registered bool
	Size       int
	Type       string
	Name       string
	Extension  string
	MimeType   string
}

type Page struct {
	PageNum  int
	PageSize int
}

type Filter struct {
	Key   string
	Term  string
	Value string
}

type Sort struct {
	Key   string
	Order string
}

var keyPrefix = "file"

func New(storer storage.StateStorer) Interface {
	return &fileStore{
		stateStore: storer,
		files:      make(map[string]FileView),
	}
}

func (fs *fileStore) Init() error {
	if err := fs.stateStore.Iterate(keyPrefix, func(k, v []byte) (bool, error) {
		if !strings.HasPrefix(string(k), keyPrefix) {
			return true, nil
		}
		key := string(k)
		var fv FileView
		if err := fs.stateStore.Get(key, &fv); err != nil {
			return false, err
		}
		fs.put(fv)
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}

func (fs *fileStore) Get(reference boson.Address) (FileView, bool) {
	file, ok := fs.files[reference.String()]
	return file, ok
}

func (fs *fileStore) GetList(page Page, filter []Filter, sort Sort) []FileView {
	ff := filterFile(fs.files, filter)
	sf := sortFile(ff, sort.Key, sort.Order)
	pf := pageFile(sf, page)
	return pf
}

func (fs *fileStore) Update(file FileView) error {
	fs.files[file.RootCid.String()] = file
	if err := fs.stateStore.Put(keyPrefix+"-"+file.RootCid.String(), file); err != nil {
		return err
	}
	return nil
}

func (fs *fileStore) Put(file FileView) error {
	exists := fs.Has(file.RootCid)
	if exists {
		return nil
	}
	fs.files[file.RootCid.String()] = file
	if err := fs.stateStore.Put(keyPrefix+"-"+file.RootCid.String(), file); err != nil {
		return err
	}
	return nil
}

func (fs *fileStore) put(file FileView) {
	exists := fs.Has(file.RootCid)
	if exists {
		return
	}
	fs.files[file.RootCid.String()] = file
}

func (fs *fileStore) Delete(reference boson.Address) error {
	exists := fs.Has(reference)
	if !exists {
		return nil
	}
	delete(fs.files, reference.String())
	if err := fs.stateStore.Delete(keyPrefix + "-" + reference.String()); err != nil {
		return err
	}
	return nil
}

func (fs *fileStore) Has(reference boson.Address) bool {
	_, ok := fs.files[reference.String()]
	return ok
}
