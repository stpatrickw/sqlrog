package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"sort"
	"text/template"
)

const (
	CORE_ELEMENT_TABLE_NAME        = "table"
	CORE_ELEMENT_TABLE_PLURAL_NAME = "tables"
	TABLE_PRIORITY                 = 10
)

type Table struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string                       `yaml:"name"`
	Fields                   map[string]*TableColumn      `yaml:"columns"`
	Indexes                  map[string]map[string]*Index `yaml:"indexes"`
	Triggers                 map[string]*Trigger          `yaml:"triggers"`
	Charset                  string
	Collate                  string
	Engine                   string
}

func (t *Table) GetName() string {
	return t.Name
}

func (t *Table) GetTypeName() string {
	return CORE_ELEMENT_TABLE_NAME
}

func (t *Table) GetPluralTypeName() string {
	return CORE_ELEMENT_TABLE_PLURAL_NAME
}

func (t *Table) AlterDefinition(t2 interface{}, sep string) []string {
	var definitions []string
	other := t.CastType(t2)
	my := &MysqlEngine{}
	var diffs []*sqlrog.DiffObject
	if !my.Equals(t.Fields, other.Fields) {
		diffs = append(diffs, my.CompareScheme(t.Fields, other.Fields)...)
	}
	for _, indexType := range IndexTypes() {
		if !my.Equals(t.Indexes[indexType], other.Indexes[indexType]) {
			diffs = append(diffs, my.CompareScheme(t.Indexes[indexType], other.Indexes[indexType])...)
		}
	}
	if !my.Equals(t.Triggers, other.Triggers) {
		diffs = append(diffs, my.CompareScheme(t.Triggers, other.Triggers)...)
	}
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].Priority > diffs[j].Priority
	})
	for _, diff := range diffs {
		if diff.Type == "table_column" {
			definitions = append(definitions, t.DiffColumnDefinition(diff, sep)...)
		} else {
			definitions = append(definitions, diff.DiffSql(sep)...)
		}
	}

	return definitions
}

func (t *Table) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s%s", t.Definition(), sep)}
}

func (t *Table) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP TABLE %s%s", t.Name, sep)}
}

func (t *Table) Definition() string {
	tableTmpl, err := template.New("table").Parse(`TABLE {{ .Name }} (
	{{$first := true}}{{range .Fields }}{{if $first}}{{$first = false}}{{else}},
	{{end}}{{ .Name }} {{ .Type }}{{if ne .Charset "" }} CHARACTER SET {{ .Charset }}{{end}}{{if ne .Collate "" }} COLLATE {{ .Collate }}{{end}}{{if .NotNull }} NOT NULL{{end}}{{if .UseDefault }} DEFAULT '{{ .Default }}'{{end}}{{if ne .Comment "" }} COMMENT '{{ .Comment }}'{{end}}{{if ne .Extra "" }} {{ .Extra }}{{end}}{{end}}{{if ne .PrimaryKeyFields ""}},
	PRIMARY KEY({{.PrimaryKeyFields}}){{end}}
) Engine={{.Engine}}{{ if ne .Charset ""}} CHARSET={{.Charset}}{{end}}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	table := &struct {
		Name             string
		Fields           []*TableColumn
		PrimaryKeyFields string
		Charset          string
		Collate          string
		Engine           string
	}{
		Name:    t.Name,
		Fields:  OrderedColumnFields(t.Fields),
		Charset: t.Charset,
		Collate: t.Collate,
		Engine:  t.Engine,
	}
	if primary := t.Indexes[PRIMARY_KEY]; primary != nil {
		for _, primaryKey := range primary {
			table.PrimaryKeyFields = OrderedIndexFields(primaryKey.Fields)
		}
	}
	err = tableTmpl.Execute(&tpl, table)
	if err != nil {
		return ""
	}

	return tpl.String()
}

func (t *Table) DiffColumnDefinition(diff *sqlrog.DiffObject, sep string) []string {
	var definitions []string
	switch diff.State {
	case sqlrog.DIFF_TYPE_CREATE:
		column := diff.To.(*TableColumn)
		definition := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", t.Name, column.Name, column.Type)
		if column.Charset != "" {
			definition += " CHARACTER SET " + column.Charset
		}
		if column.Collate != "" {
			definition += " COLLATE " + column.Collate
		}
		if column.NotNull {
			definition += " NOT NULL"
		}
		if column.Default != "" {
			definition += " DEFAULT " + column.Default
		}
		if column.Comment != "" {
			definition += " COMMENT '" + column.Comment + "'"
		}
		definitions = append(definitions, definition+sep)
	case sqlrog.DIFF_TYPE_DROP:
		column := diff.From.(*TableColumn)
		definitions = append(definitions, fmt.Sprintf("ALTER TABLE %s DROP %s%s", t.Name, column.Name, sep))
	case sqlrog.DIFF_TYPE_UPDATE:
		column := diff.From.(*TableColumn)
		definition := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s %s", t.Name, column.Name, column.Name, column.Type)
		if column.Charset != "" {
			definition += " CHARACTER SET " + column.Charset
		}
		if column.Collate != "" {
			definition += " COLLATE " + column.Collate
		}
		if column.NotNull {
			definition += " NOT NULL"
		}
		if column.Default != "" {
			definition += " DEFAULT " + column.Default
		}
		if column.Comment != "" {
			definition += " COMMENT '" + column.Comment + "'"
		}
		definitions = append(definitions, definition+sep)
	}
	return definitions
}

func (t *Table) CommentOnColumn(column *TableColumn, sep string) string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'%s\n", t.Name, column.Name, column.Comment, sep)
}

func (t *Table) Equals(t2 interface{}) bool {
	other := t.CastType(t2)
	my := &MysqlEngine{}

	if !my.Equals(t.Fields, other.Fields) {
		return false
	}

	for _, indexType := range IndexTypes() {
		if !my.Equals(t.Indexes[indexType], other.Indexes[indexType]) {
			return false
		}
	}

	if !my.Equals(t.Triggers, other.Triggers) {
		return false
	}

	return true
}

func (t *Table) Diff(t2 interface{}) *sqlrog.DiffObject {
	other := t.CastType(t2)

	if !t.Equals(other) {
		return &sqlrog.DiffObject{
			State:    sqlrog.DIFF_TYPE_UPDATE,
			Type:     t.GetTypeName(),
			From:     t,
			To:       other,
			Priority: TABLE_PRIORITY,
		}
	}

	return nil
}

func (t *Table) DiffsOnCreate(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	var diffs []*sqlrog.DiffObject
	diffs = append(diffs, &sqlrog.DiffObject{
		State:    sqlrog.DIFF_TYPE_CREATE,
		Type:     t.GetTypeName(),
		From:     nil,
		To:       t,
		Priority: TABLE_PRIORITY * sqlrog.DIFF_TYPE_CREATE,
	})

	fb := &MysqlEngine{}
	for typeName, indexesByType := range t.Indexes {
		if typeName != PRIMARY_KEY {
			diffs = append(diffs, fb.CompareScheme(indexesByType, nil)...)
		}
	}
	diffs = append(diffs, fb.CompareScheme(t.Triggers, nil)...)

	return diffs
}

func (t *Table) CastType(other interface{}) *Table {
	return other.(*Table)
}

func (t *Table) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	tablesMap := make(map[string]*Table)
	rows, err := conn.Query(`
		SELECT t.table_name, t.engine, t.table_collation, c.character_set_name 
        FROM INFORMATION_SCHEMA.TABLES t 
        LEFT JOIN INFORMATION_SCHEMA.COLLATION_CHARACTER_SET_APPLICABILITY c ON c.COLLATION_NAME=t.TABLE_COLLATION
        where table_schema = schema() AND TABLE_TYPE = 'BASE TABLE'
        order by t.table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		table := &Table{Fields: make(map[string]*TableColumn), Indexes: make(map[string]map[string]*Index), Triggers: make(map[string]*Trigger)}
		err := rows.Scan(&table.Name, &table.Engine, &table.Collate, &table.Charset)
		if err != nil {
			return nil, err
		}
		tablesMap[table.Name] = table
	}
	tableFieldEntity := &TableColumn{}
	tableFields, err := tableFieldEntity.FetchColumnsFromDB(conn)
	if err != nil {
		return nil, err
	}
	for tableName, fieldsByTable := range tableFields {
		if ok := tablesMap[tableName]; ok == nil {
			return nil, err
		}
		tablesMap[tableName].Fields = fieldsByTable
	}
	triggerEntity := &Trigger{}
	triggers, err := triggerEntity.FetchTriggersFromDB(conn)
	if err != nil {
		return nil, err
	}
	for tableName, triggersByTable := range triggers {
		if ok := tablesMap[tableName]; ok == nil {
			return nil, err
		}
		tablesMap[tableName].Triggers = triggersByTable
	}
	indexEntity := &Index{}
	indexes, err := indexEntity.FetchIndexesFromDB(conn)
	if err != nil {
		return nil, err
	}
	for tableName, indexesByTable := range indexes {
		if ok := tablesMap[tableName]; ok == nil {
			return nil, err
		}
		tablesMap[tableName].Indexes = indexesByTable
	}
	var tables []sqlrog.ElementSchema
	for _, table := range tablesMap {
		tables = append(tables, table)
	}

	return tables, nil
}

func OrderedColumnFields(fields map[string]*TableColumn) []*TableColumn {
	var columnFields []*TableColumn
	for _, columnField := range fields {
		columnFields = append(columnFields, columnField)
	}
	sort.Slice(columnFields, func(i, j int) bool {
		return columnFields[i].Position < columnFields[j].Position
	})

	return columnFields
}
