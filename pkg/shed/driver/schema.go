package driver

// FieldSpec holds information about a particular field.
type FieldSpec struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type"`
}

// IndexSpec holds information about a particular index.
// It does not contain index type, as indexes do not have type.
type IndexSpec struct {
	Name   string `json:"name"`
	Prefix []byte `json:"prefix"`
}

// SchemaSpec is used to serialize known database structure information.
type SchemaSpec struct {
	Fields  []FieldSpec `json:"fields"`  // keys are field names
	Indexes []IndexSpec `json:"indexes"` // keys are index prefix bytes
}

type Schema interface {
	DefaultFieldKey() []byte
	DefaultIndexKey() []byte
	InitSchema() error
	GetSchemaSpec() (SchemaSpec, error)
	CreateField(spec FieldSpec) ([]byte, error)
	CreateIndex(spec IndexSpec) ([]byte, error)
	RenameIndex(oldName, newName string) (bool, error)
}
