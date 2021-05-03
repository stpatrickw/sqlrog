package sqlrog

import "database/sql"

type ElementSchema interface {
	GetName() string
	GetTypeName() string
	GetPluralTypeName() string
	FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error)
	AddChild(child ElementSchema) error
	GetChilds() []ElementSchema
	CreateDefinition(separator string) []string
	AlterDefinition(other interface{}, separator string) []string
	DropDefinition(separator string) []string
	Equals(other interface{}) bool
	Diff(other interface{}) *DiffObject
	DiffsOnCreate(schema ElementSchema) []*DiffObject
	DiffsOnDrop(schema ElementSchema) []*DiffObject
	GetPriority() int
	GetGlobalChildElements() []ElementSchema
}

type BaseElementSchema struct {
	CoreElements map[string]map[string]ElementSchema `yaml:"coreelements,omitempty"`
}

func (be *BaseElementSchema) AddChild(child ElementSchema) error {
	return nil
}
func (be *BaseElementSchema) GetChilds() []ElementSchema {
	return nil
}
func (be *BaseElementSchema) GetGlobalChildElements() []ElementSchema {
	return nil
}
func (be *BaseElementSchema) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	return nil, nil
}
func (be *BaseElementSchema) GetName() string {
	return ""
}
func (be *BaseElementSchema) GetTypeName() string {
	return ""
}
func (be *BaseElementSchema) GetPluralTypeName() string {
	return ""
}
func (be *BaseElementSchema) AlterDefinition(fbs2 interface{}, sep string) []string {
	return []string{}
}
func (be *BaseElementSchema) CreateDefinition(sep string) []string {
	return []string{}
}
func (be *BaseElementSchema) Equals(fbs2 interface{}) bool {
	return true
}
func (be *BaseElementSchema) DropDefinition(sep string) []string {
	return []string{}
}
func (be *BaseElementSchema) Diff(fbs2 interface{}) *DiffObject {
	return nil
}
func (be *BaseElementSchema) GetPriority() int {
	return 0
}
func (be *BaseElementSchema) DiffsOnCreate(schema ElementSchema) []*DiffObject {
	return []*DiffObject{
		{
			State:    DIFF_TYPE_CREATE,
			Type:     schema.GetTypeName(),
			From:     nil,
			To:       schema,
			Priority: schema.GetPriority(),
		},
	}
}
func (be *BaseElementSchema) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return []*DiffObject{
		{
			State:    DIFF_TYPE_DROP,
			Type:     schema.GetTypeName(),
			From:     schema,
			To:       nil,
			Priority: schema.GetPriority(),
		},
	}
}
