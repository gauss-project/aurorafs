// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package shed

import (
	"errors"

	"github.com/gauss-project/aurorafs/pkg/shed/driver"
)

var (
	fieldKeyPrefixLength = 0
	indexKeyPrefixLength = 0
)

func (db *DB) initSchema() error {
	err := db.backend.InitSchema()
	if err != nil {
		return err
	}

	fieldKeyPrefixLength = len(db.backend.DefaultFieldKey())
	indexKeyPrefixLength = len(db.backend.DefaultIndexKey())

	return nil
}

// schemaFieldKey retrieves the complete LevelDB key for
// a particular field from the schema definition.
func (db *DB) schemaFieldKey(name, fieldType string) (key []byte, err error) {
	if name == "" {
		return nil, errors.New("field name cannot be blank")
	}
	if fieldType == "" {
		return nil, errors.New("field type cannot be blank")
	}
	return db.backend.CreateField(driver.FieldSpec{
		Name: name,
		Type: fieldType,
	})
}

// RenameIndex changes the schema so that an existing index name is changed
// while preserving its data by keeping the same internal key prefix.
// Renaming indexes is useful when encoding functions can be backward compatible
// to avoid data migrations.
func (db *DB) RenameIndex(name, newName string) (renamed bool, err error) {
	if name == "" {
		return false, errors.New("index name cannot be blank")
	}
	if newName == "" {
		return false, errors.New("new index name cannot be blank")
	}
	if newName == name {
		return false, nil
	}
	return db.backend.RenameIndex(name, newName)
}

// schemaIndexID retrieves the complete LevelDB prefix for
// a particular index.
func (db *DB) schemaIndexPrefix(name string) (id []byte, err error) {
	if name == "" {
		return nil, errors.New("index name cannot be blank")
	}
	return db.backend.CreateIndex(driver.IndexSpec{
		Name: name,
	})
}

// getSchema retrieves the complete schema from
// the database.
func (db *DB) getSchema() (s driver.SchemaSpec, err error) {
	return db.backend.GetSchemaSpec()
}
