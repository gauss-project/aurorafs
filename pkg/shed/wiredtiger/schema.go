package wiredtiger

import (
	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

const (
	schemaMetadataTableName = "schema_metadata"

	fieldTableName              = "fields_collection"
	fieldMetadataKeyPrefix      = "field_"
	fieldKeyPrefix         byte = 1

	indexMetadataPlaceholder = "index"
	indexMetadataKeyPrefix   = "index_"
	indexTablePrefix         = "ind_col_"

	stateTableName = "state_collection"
)

var indexKeyPrefix byte = 48

func (db *DB) initSchema() error {
	s := db.pool.Get()
	defer db.pool.Put(s)

	tableOption := &createOption{
		SourceType:        "file",
		MemoryPageMax:     10485760, // 10M
		SplitPct:          90,
		LeafPageMax:       64 * 1024,
		LeafValueMax:      67108864, // 64MB
		Checksum:          "on",
		PrefixCompression: true,
		BlockCompressor:   NoneCompressor,
		AccessPatternHint: "random",
		AllocationSize:    4 * 1024,
		KeyFormat:         "u",
		ValueFormat:       "u",
	}

	// create metadata
	err := s.create(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, tableOption)
	if err != nil {
		return err
	}

	// all fields should store at fieldTable
	err = s.create(dataSource{dataType: tableSource, sourceName: fieldTableName}, tableOption)
	if err != nil {
		return err
	}

	// no prefix key should store at stateTable
	err = s.create(dataSource{dataType: tableSource, sourceName: stateTableName}, tableOption)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) GetFieldKey() []byte {
	return []byte{fieldKeyPrefix}
}

func (db *DB) GetIndexKey() []byte {
	return []byte{indexKeyPrefix}
}

func (db *DB) CreateField(spec driver.FieldSpec) ([]byte, error) {
	s := db.pool.Get()
	defer db.pool.Put(s)
	c, err := s.openCursor(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, &cursorOption{Overwrite: false})
	if err != nil {
		return nil, err
	}
	key := append([]byte(fieldMetadataKeyPrefix), []byte(spec.Name)...)
	err = c.insert(key, []byte(spec.Type))
	switch {
	case err == nil:
		fallthrough
	case IsDuplicateKey(err):
		return append([]byte{fieldKeyPrefix}, []byte(spec.Name)...), nil
	}
	return nil, err
}

func (db *DB) CreateIndex(spec driver.IndexSpec) ([]byte, error) {
	s := db.pool.Get()
	defer db.pool.Put(s)
	c, err := s.openCursor(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, &cursorOption{Overwrite: false})
	if err != nil {
		return nil, err
	}
	key := append([]byte(indexMetadataKeyPrefix), []byte(spec.Name)...)
	prefix := indexKeyPrefix
	err = c.insert(key, []byte(indexMetadataPlaceholder))
	switch {
	case err == nil:
		// create index table
		err = s.create(dataSource{dataType: tableSource, sourceName: string(append([]byte(indexTablePrefix), prefix))}, &createOption{
			SourceType:        "file",
			MemoryPageMax:     10485760, // 10M
			SplitPct:          90,
			LeafPageMax:       64 * 1024,
			LeafValueMax:      67108864, // 64MB
			Checksum:          "on",
			PrefixCompression: true,
			BlockCompressor:   SnappyCompressor,
			AccessPatternHint: "random",
			AllocationSize:    4 * 1024,
			KeyFormat:         "u",
			ValueFormat:       "u",
		})
		if err != nil {
			return nil, err
		}
		fallthrough
	case IsDuplicateKey(err):
		indexKeyPrefix++
		return []byte{prefix}, nil
	}
	return nil, err
}

func (db *DB) RenameIndex(oldName, newName string) (bool, error) {
	s := db.pool.Get()
	defer db.pool.Put(s)
	c, err := s.openCursor(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, &cursorOption{Overwrite: false})
	if err != nil {
		return false, err
	}
	key := append([]byte(indexMetadataKeyPrefix), []byte(oldName)...)
	r, err := c.find(key)
	switch {
	case err == nil:
	case IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
	defer r.Close()
	err = c.remove(key)
	if err != nil {
		return false, err
	}
	newKey := append([]byte(indexMetadataKeyPrefix), []byte(newName)...)
	err = c.insert(newKey, []byte(indexMetadataPlaceholder))
	switch {
	case err == nil:
		return true, nil
	case IsDuplicateKey(err):
		return false, nil
	}
	return false, err
}
