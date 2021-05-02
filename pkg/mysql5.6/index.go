package mysql

import (
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
	"sort"
	"strings"
)

const (
	PRIMARY_KEY          = "PRIMARY KEY"
	FOREIGN_KEY          = "FOREIGN KEY"
	UNIQUE               = "UNIQUE"
	INDEX                = "INDEX"
	PRIMARY_KEY_PRIORITY = 9
	FOREIGN_KEY_PRIORITY = 7
	UNIQUE_PRIORITY      = 8
	INDEX_PRIORITY       = 8
)

type Index struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string
	Type              string
	Algorithm         string
	Unique            bool
	TableName         string
	Fields            map[string]IndexField
	SourceTable       string
	SourceFields      map[string]IndexField
	OnDelete          string
	OnUpdate          string
}

type IndexField struct {
	Name     string
	Position int
}

func (i *Index) GetTypeName() string {
	return "index"
}

func (i *Index) AlterDefinition(other interface{}, sep string) []string {
	i2 := i.CastType(other)
	definitions := i.DropDefinition(sep)
	definitions = append(definitions, i2.CreateDefinition(sep)...)

	return definitions
}

func (i *Index) CreateDefinition(sep string) []string {
	definitions := []string{i.Definition(sep)}

	return definitions
}

func (i *Index) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("ALTER TABLE %s DROP %s %s%s", i.TableName, i.Type, i.Name, sep)}
}

func (i *Index) Definition(sep string) string {

	var definition string

	switch i.Type {
	case PRIMARY_KEY:
		definition = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s (%s)", i.TableName, i.Type, OrderedIndexFields(i.Fields))
	case UNIQUE:
		definition = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s (%s)", i.TableName, i.Name, i.Type, OrderedIndexFields(i.Fields))
	case FOREIGN_KEY:
		definition = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s (%s) REFERENCES %s (%s)",
			i.TableName, i.Name, i.Type, OrderedIndexFields(i.Fields), i.SourceTable, OrderedIndexFields(i.SourceFields))
		if i.OnDelete != "" {
			definition += " ON DELETE " + i.OnDelete
		}
		if i.OnUpdate != "" {
			definition += " ON UPDATE " + i.OnUpdate
		}
	case INDEX:
		unique := ""
		if i.Unique {
			unique = " UNIQUE"
		}
		order := ""

		definition = fmt.Sprintf("CREATE%s%s INDEX %s ON %s (%s)", unique, order, i.Name, i.TableName, OrderedIndexFields(i.Fields))
	}

	return definition + sep
}

func OrderedIndexFields(fields map[string]IndexField) string {
	var indexFields []IndexField
	for _, indexField := range fields {
		indexFields = append(indexFields, indexField)
	}
	sort.Slice(indexFields, func(i, j int) bool {
		return indexFields[i].Position < indexFields[j].Position
	})
	var stringFields []string
	for _, index := range indexFields {
		stringFields = append(stringFields, index.Name)
	}

	return strings.Join(stringFields, ",")
}

func (i *Index) Equals(i2 interface{}) bool {
	other := i.CastType(i2)

	if i.Name != other.Name || i.TableName != other.TableName || i.SourceTable != other.SourceTable ||
		i.OnDelete != other.OnDelete || i.OnUpdate != other.OnUpdate {
		return false
	}

	if !IndexFieldsEqual(i.Fields, other.Fields) {
		return false
	}

	if !IndexFieldsEqual(i.SourceFields, other.SourceFields) {
		return false
	}

	return true
}

func (i *Index) Diff(i2 interface{}) *DiffObject {
	other := i.CastType(i2)

	if !i.Equals(other) {
		return &DiffObject{
			State:    DIFF_TYPE_UPDATE,
			Type:     i.String(),
			From:     i,
			To:       other,
			Priority: i.GetPriorityByType(i.Type, false),
		}
	}

	return nil
}

func (i *Index) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return []*DiffObject{
		{
			State:    DIFF_TYPE_DROP,
			Type:     i.GetTypeName(),
			From:     i,
			To:       nil,
			Priority: i.GetPriorityByType(i.Type, true),
		},
	}
}

func IndexFieldsEqual(src map[string]IndexField, dest map[string]IndexField) bool {
	if len(src) != len(dest) {
		return false
	}
	for name, param := range src {
		if _, ok := dest[name]; !ok {
			return false
		}
		if !IndexFieldEquals(param, dest[name]) {
			return false
		}
	}
	for name, _ := range dest {
		if _, ok := src[name]; !ok {
			return false
		}
	}

	return true
}

func IndexFieldEquals(src IndexField, dest IndexField) bool {
	return src.Name == dest.Name && src.Position == dest.Position
}

func (i *Index) CastType(other interface{}) *Index {
	return other.(*Index)
}

func (i *Index) String() string {
	return i.Type
}

func IndexTypes() []string {
	return []string{INDEX, PRIMARY_KEY, FOREIGN_KEY, UNIQUE}
}

func (i *Index) GetPriorityByType(Type string, drop bool) int {
	switch Type {
	case PRIMARY_KEY:
		return PRIMARY_KEY_PRIORITY
	case FOREIGN_KEY:
		if drop {
			return FOREIGN_KEY_PRIORITY + 10
		}
		return FOREIGN_KEY_PRIORITY
	case UNIQUE:
		return UNIQUE_PRIORITY
	default:
		return INDEX_PRIORITY
	}
}

func (i *Index) FetchIndexesFromDB(conn *sql.DB) (map[string]map[string]map[string]*Index, error) {

	indexQuery := `
		select i.table_name, i.index_name, i.non_unique, 
			i.seq_in_index as position, i.column_name, i.index_type, 
            coalesce(c.constraint_type, 'INDEX'), '', '', 0, '', ''
        from INFORMATION_SCHEMA.STATISTICS i
		left join INFORMATION_SCHEMA.TABLE_CONSTRAINTS c on i.index_name = c.constraint_name and i.table_schema = c.constraint_schema
        WHERE i.table_schema = schema()
        union all 
        select c.table_name, c.constraint_name as index_name, 1 as non_unique,
			k.ordinal_position as position, k.column_name, '' as index_type,
            c.constraint_type, r.update_rule, r.delete_rule, k.position_in_unique_constraint, k.referenced_table_name, k.referenced_column_name
        from INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
        join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r on r.constraint_schema = c.constraint_schema and r.constraint_name = c.constraint_name
        join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k on k.constraint_schema = c.constraint_schema and k.constraint_name = c.constraint_name
        where c.constraint_schema = schema() and c.constraint_type = 'FOREIGN KEY'`
	indexRows, err := conn.Query(indexQuery)
	if err != nil {
		return nil, err
	}

	tableIndexes := make(map[string]map[string]map[string]*Index)
	for indexRows.Next() {
		var (
			nonUnique int
		)
		tableIndex := Index{Fields: make(map[string]IndexField), SourceFields: make(map[string]IndexField)}
		indexField := &IndexField{}
		sourceField := &IndexField{}
		err := indexRows.Scan(&tableIndex.TableName,
			&tableIndex.Name,
			&nonUnique,
			&indexField.Position,
			&indexField.Name,
			&tableIndex.Algorithm,
			&tableIndex.Type,
			&tableIndex.OnUpdate,
			&tableIndex.OnDelete,
			&sourceField.Position,
			&tableIndex.SourceTable,
			&sourceField.Name)
		if err != nil {
			return nil, err
		}
		if nonUnique == 0 {
			tableIndex.Unique = true
		}
		if _, ok := tableIndexes[tableIndex.TableName]; !ok {
			tableIndexes[tableIndex.TableName] = make(map[string]map[string]*Index)
		}
		if _, ok := tableIndexes[tableIndex.TableName][tableIndex.Type]; !ok {
			tableIndexes[tableIndex.TableName][tableIndex.Type] = make(map[string]*Index)
		}
		if _, ok := tableIndexes[tableIndex.TableName][tableIndex.Type][tableIndex.Name]; !ok {
			tableIndexes[tableIndex.TableName][tableIndex.Type][tableIndex.Name] = &tableIndex
		}
		if tableIndex.Type == "FOREIGN KEY" {
			tableIndexes[tableIndex.TableName][tableIndex.Type][tableIndex.Name].SourceFields[sourceField.Name] = *sourceField
		}
		tableIndexes[tableIndex.TableName][tableIndex.Type][tableIndex.Name].Fields[indexField.Name] = *indexField
	}

	indexRows.Close()

	return tableIndexes, nil
}
