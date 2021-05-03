package fb

import (
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
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
	INDEX_PRIORITY       = 8
)

type Index struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string
	Type                     string
	Unique                   bool
	TableName                string
	Computed                 bool
	Expression               string
	Fields                   map[string]IndexField
	SourceTable              string
	SourceFields             map[string]IndexField
	Comment                  string
	OnDelete                 string
	OnUpdate                 string
	Asc                      bool
	Active                   bool
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
	if !i.Active {
		definitions = append(definitions, i.ActivityDefinition(sep))
	}
	return definitions
}

func (i *Index) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP INDEX %s%s", i.Name, sep)}
}

func (i *Index) ActivityDefinition(sep string) string {
	if i.Active {
		fmt.Sprintf("ALTER INDEX %s ACTIVE%s", i.Name, sep)
	}

	return fmt.Sprintf("ALTER INDEX %s INACTIVE%s", i.Name, sep)
}

func (i *Index) Definition(sep string) string {

	var definition string

	switch i.Type {
	case PRIMARY_KEY, UNIQUE:
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
		var (
			unique   string
			computed string
		)
		fieldsDefinition := OrderedIndexFields(i.Fields)
		if i.Unique {
			unique = " UNIQUE"
		}
		if i.Computed && len(i.Expression) > 1 {
			computed = " COMPUTED BY"
			fieldsDefinition = i.Expression
		}
		order := ""
		if !i.Asc {
			order = " DESCENDING"
		}
		definition = fmt.Sprintf("CREATE%s%s INDEX %s ON %s%s (%s)", unique, order, i.Name, i.TableName, computed, fieldsDefinition)
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

	if i.Name != other.Name || i.TableName != other.TableName || i.Computed != other.Computed ||
		i.Expression != other.Expression || i.SourceTable != other.SourceTable || i.Comment != other.Comment ||
		i.OnDelete != other.OnDelete || i.OnUpdate != other.OnUpdate || i.Asc != other.Asc || i.Active != other.Active {
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

func (i *Index) GetPriorityByType(Type string) int {
	switch Type {
	case PRIMARY_KEY:
		return PRIMARY_KEY_PRIORITY
	case FOREIGN_KEY:
		return FOREIGN_KEY_PRIORITY
	default:
		return INDEX_PRIORITY
	}
}

func (i *Index) Diff(i2 interface{}) *sqlrog.DiffObject {
	other := i.CastType(i2)

	if !i.Equals(other) {
		return &sqlrog.DiffObject{
			State:    sqlrog.DIFF_TYPE_UPDATE,
			Type:     i.String(),
			From:     i,
			To:       other,
			Priority: i.GetPriorityByType(i.Type),
		}
	}

	return nil
}

func (i *Index) DiffsOnCreate(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return []*sqlrog.DiffObject{
		{
			State:    sqlrog.DIFF_TYPE_CREATE,
			Type:     i.GetTypeName(),
			From:     nil,
			To:       i,
			Priority: i.GetPriorityByType(i.Type),
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

func (i *Index) FetchIndexesFromDB(conn *sql.DB) (map[string]map[string]map[string]*Index, error) {

	indexQuery := `select trim(i.rdb$relation_name), trim(coalesce(i.rdb$index_name, '')),
            trim(coalesce(i2.rdb$relation_name,'')),  trim(coalesce(s2.rdb$field_name,'')),
            trim(coalesce(c.rdb$constraint_type, 'INDEX')) as index_type,
            case i.rdb$segment_count when 0 then 1 else 0 end as index_computed,
            trim(coalesce(i.rdb$expression_source, '')),
            trim(coalesce(i.rdb$description, '')),
            case i.rdb$index_type when 0 then 1 else 0 end as is_asc,
            case coalesce(i.rdb$index_inactive, 0) when 0 then 1 else 0 end as index_active,
                trim(coalesce(s.rdb$field_name, '')),
                coalesce(s.rdb$field_position, 0),
                   coalesce(s2.rdb$field_position, 0),
                case when trim(rf.rdb$update_rule) = 'RESTRICT' then '' else trim(coalesce(rf.rdb$update_rule, '')) end,
                case when trim(rf.rdb$delete_rule) = 'RESTRICT' then '' else trim(coalesce(rf.rdb$delete_rule, '')) end,
                   coalesce(i.rdb$unique_flag, 0)
            from rdb$indices i
            left join rdb$index_segments s on s.rdb$index_name = i.rdb$index_name
            left join rdb$relation_constraints c on c.rdb$constraint_name = i.rdb$index_name
            left join rdb$indices i2 on i.rdb$foreign_key = i2.rdb$index_name
            left join rdb$index_segments s2 on s2.rdb$index_name = i.rdb$foreign_key
            left join rdb$ref_constraints rf on rf.rdb$constraint_name = i.rdb$index_name
            WHERE i.rdb$system_flag = 0 
            ORDER BY i.rdb$index_name`
	indexRows, err := conn.Query(indexQuery)
	if err != nil {
		return nil, err
	}

	tableIndexes := make(map[string]map[string]map[string]*Index)
	for indexRows.Next() {
		tableIndex := Index{Fields: make(map[string]IndexField), SourceFields: make(map[string]IndexField)}
		indexField := &IndexField{}
		sourceField := &IndexField{}
		err := indexRows.Scan(&tableIndex.TableName,
			&tableIndex.Name,
			&tableIndex.SourceTable,
			&sourceField.Name,
			&tableIndex.Type,
			&tableIndex.Computed,
			&tableIndex.Expression,
			&tableIndex.Comment,
			&tableIndex.Asc,
			&tableIndex.Active,
			&indexField.Name,
			&indexField.Position,
			&sourceField.Position,
			&tableIndex.OnUpdate,
			&tableIndex.OnDelete,
			&tableIndex.Unique)
		if err != nil {
			return nil, err
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

func (i *Index) DiffsOnDrop(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return i.BaseElementSchema.DiffsOnDrop(schema)
}
