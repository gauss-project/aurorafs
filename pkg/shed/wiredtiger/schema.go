package wiredtiger

import (
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

const (
	schemaMetadataTableName = "schema_metadata"

	fieldTableName              = "fields_collection"
	fieldMetadataKeyPrefix      = "field_"
	fieldKeyPrefix         byte = 1

	indexMetadataKeyPrefix = "index_"
	indexTablePrefix       = "ind_col_"

	stateTableName = "state_collection"
)

var (
	// Every index has its own key prefix and this value defines the first one.
	defaultIndexKeyPrefix = []byte{'0'}
)

func indexKeyPrefixIncr(prefix []byte) error {
	carry := 0
	carryFn := func(b *byte) int {
		carry := 0
		switch *b {
		case '9':
			*b = 'a'
		case 'z':
			carry = 1
			*b = '0'
		default:
			*b++
		}
		return carry
	}
	for i := len(prefix) - 1; i >= 0; i-- {
		if carry == 1 {
			carry = carryFn(&prefix[i])
		}
		carry = carryFn(&prefix[i])
		if carry == 0 {
			break
		}
	}
	if carry == 1 {
		return fmt.Errorf("reach maximum table number")
	}
	return nil
}

var schema driver.SchemaSpec

func (db *DB) InitSchema() error {
	s, err := newSession(db.conn)
	if err != nil {
		return err
	}

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
	err = s.create(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, tableOption)
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

	schema.Fields = make([]driver.FieldSpec, 0)
	schema.Indexes = make([]driver.IndexSpec, 0)

	return s.close()
}

func (db *DB) DefaultFieldKey() []byte {
	return []byte{fieldKeyPrefix}
}

func (db *DB) DefaultIndexKey() []byte {
	return defaultIndexKeyPrefix
}

func (db *DB) CreateField(spec driver.FieldSpec) ([]byte, error) {
	var found bool
	for _, f := range schema.Fields {
		if f.Name == spec.Name {
			if f.Type != spec.Type {
				return nil, fmt.Errorf("field %q of type %q stored as %q in db", spec.Name, spec.Type, f.Type)
			}
			break
		}
	}
	if !found {
		obj := dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}
		s := db.pool.Get(obj.String())
		if s == nil {
			return nil, ErrSessionHasClosed
		}
		defer db.pool.Put(s)
		c, err := s.openCursor(obj, &cursorOption{Overwrite: false})
		if err != nil {
			return nil, err
		}
		defer s.closeCursor(c)
		key := append([]byte(fieldMetadataKeyPrefix), []byte(spec.Name)...)
		err = c.insert(key, []byte(spec.Type))
		switch {
		case IsDuplicateKey(err):
			// nothing
		case err == nil:
		case err != nil:
			return nil, err
		}
		schema.Fields = append(schema.Fields, spec)
	}
	return append([]byte{fieldKeyPrefix}, []byte(spec.Name)...), nil
}

func (db *DB) CreateIndex(spec driver.IndexSpec) ([]byte, error) {
	currentPrefix := defaultIndexKeyPrefix
	for _, i := range schema.Indexes {
		if i.Name == spec.Name {
			return i.Prefix, nil
		}
		currentPrefix = i.Prefix
	}
	obj := dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}
	s := db.pool.Get(obj.String())
	if s == nil {
		return nil, ErrSessionHasClosed
	}
	defer db.pool.Put(s)
	c, err := s.openCursor(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, &cursorOption{Overwrite: false})
	if err != nil {
		return nil, err
	}
	defer s.closeCursor(c)
	key := append([]byte(indexMetadataKeyPrefix), []byte(spec.Name)...)
	prefix := make([]byte, len(currentPrefix))
	copy(prefix, currentPrefix)
	err = c.insert(key, prefix)
	switch {
	case IsDuplicateKey(err):
		r, _ := c.find(key)
		value := r.Value()
		_ = r.Close()
		copy(prefix, value)
	case err == nil:
		err = indexKeyPrefixIncr(prefix)
		if err != nil {
			return nil, err
		}
		// create index table
		err = s.create(dataSource{dataType: tableSource, sourceName: string(append([]byte(indexTablePrefix), prefix...))}, &createOption{
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
		err = c.update(key, prefix)
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}
	spec.Prefix = make([]byte, len(prefix))
	copy(spec.Prefix, prefix)
	schema.Indexes = append(schema.Indexes, spec)
	return spec.Prefix, nil
}

func (db *DB) RenameIndex(oldName, newName string) (bool, error) {
	obj := dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}
	s := db.pool.Get(obj.String())
	if s == nil {
		return false, ErrSessionHasClosed
	}
	defer db.pool.Put(s)
	c, err := s.openCursor(dataSource{dataType: tableSource, sourceName: schemaMetadataTableName}, &cursorOption{Overwrite: false})
	if err != nil {
		return false, err
	}
	defer s.closeCursor(c)
	key := append([]byte(indexMetadataKeyPrefix), []byte(oldName)...)
	r, err := c.find(key)
	switch {
	case err == nil:
	case IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	defer r.Close()
	value := r.Value()
	err = c.remove(key)
	if err != nil {
		return false, err
	}
	newKey := append([]byte(indexMetadataKeyPrefix), []byte(newName)...)
	err = c.insert(newKey, value)
	switch {
	case err == nil:
		for d, i := range schema.Indexes {
			if i.Name == oldName {
				i.Name = newName
				schema.Indexes[d] = i
				break
			}
		}
		return true, nil
	case IsDuplicateKey(err):
		return false, nil
	}
	return false, err
}

func (db *DB) GetSchemaSpec() (driver.SchemaSpec, error) {
	return schema, nil
}
