package leveldb

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

var (
	// LevelDB key value for storing the schema.
	keySchema = []byte{0}
	// LevelDB key prefix for all field type.
	// LevelDB keys will be constructed by appending name values to this prefix.
	keyPrefixFields byte = 1
	// LevelDB key prefix from which indexing keys start.
	// Every index has its own key prefix and this value defines the first one.
	keyPrefixIndexStart byte = 2 // Q: or maybe a higher number like 7, to have more space for potential specific perfixes
)

func (l *LevelDB) DefaultFieldKey() []byte {
	return []byte{keyPrefixFields}
}

func (l *LevelDB) DefaultIndexKey() []byte {
	return []byte{keyPrefixIndexStart}
}

func (l *LevelDB) CreateField(spec driver.FieldSpec) ([]byte, error) {
	if spec.Name == "" {
		return nil, errors.New("field name cannot be blank")
	}
	if spec.Type == "" {
		return nil, errors.New("field type cannot be blank")
	}
	s, err := l.getSchema()
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	var found bool
	for _, f := range s.Fields {
		if f.Name == spec.Name {
			if f.Type != spec.Type {
				return nil, fmt.Errorf("field %q of type %q stored as %q in db", spec.Name, spec.Type, f.Type)
			}
			break
		}
	}
	if !found {
		s.Fields = append(s.Fields, spec)
		err := l.putSchema(s)
		if err != nil {
			return nil, fmt.Errorf("put schema: %w", err)
		}
	}
	return append([]byte{keyPrefixFields}, []byte(spec.Name)...), nil
}

func (l *LevelDB) CreateIndex(spec driver.IndexSpec) ([]byte, error) {
	s, err := l.getSchema()
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	nextID := keyPrefixIndexStart
	for _, f := range s.Indexes {
		if f.Prefix[0] >= nextID {
			nextID = f.Prefix[0] + 1
		}
		if f.Name == spec.Name {
			return f.Prefix, nil
		}
	}
	id := nextID
	spec.Prefix = []byte{id}
	s.Indexes = append(s.Indexes, spec)
	return spec.Prefix, l.putSchema(s)
}

func (l *LevelDB) RenameIndex(oldName, newName string) (bool, error) {
	if oldName == "" {
		return false, errors.New("index name cannot be blank")
	}
	if newName == "" {
		return false, errors.New("new index name cannot be blank")
	}
	if newName == oldName {
		return false, nil
	}
	s, err := l.getSchema()
	if err != nil {
		return false, fmt.Errorf("get schema: %w", err)
	}
	for i, f := range s.Indexes {
		if f.Name == oldName {
			s.Indexes[i].Name = newName
			return true, l.putSchema(s)
		}
		if f.Name == newName {
			return true, nil
		}
	}
	return false, nil
}

func (l *LevelDB) GetSchemaSpec() (driver.SchemaSpec, error) {
	return l.getSchema()
}

// getSchema retrieves the complete schema from
// the database.
func (l *LevelDB) getSchema() (s driver.SchemaSpec, err error) {
	b, err := l.Get(driver.Key{Data: keySchema})
	if err != nil {
		return s, err
	}
	err = json.Unmarshal(b, &s)
	return s, err
}

// putSchema stores the complete schema to
// the database.
func (l *LevelDB) putSchema(s driver.SchemaSpec) (err error) {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return l.Put(driver.Key{Data: keySchema}, driver.Value{Data: b})
}
