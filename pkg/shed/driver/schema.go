package driver

// FieldSpec holds information about a particular field.
type FieldSpec struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type"`
}

// IndexSpec holds information about a particular index.
// It does not contain index type, as indexes do not have type.
type IndexSpec struct {
	Name string `json:"name"`
}

type Schema interface {
	GetFieldKey() []byte
	GetIndexKey() []byte
	CreateField(spec FieldSpec) ([]byte, error)
	CreateIndex(spec IndexSpec) ([]byte, error)
	RenameIndex(oldName, newName string) (bool, error)
}
