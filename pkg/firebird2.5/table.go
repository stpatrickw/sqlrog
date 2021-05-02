package fb

import (
	"bytes"
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
	"text/template"
)

const (
	CORE_ELEMENT_TABLE_NAME        = "table"
	CORE_ELEMENT_TABLE_PLURAL_NAME = "tables"
	TABLE_PRIORITY                 = 10
)

type Table struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string                       `yaml:"name"`
	Fields            map[string]*TableColumn      `yaml:"columns"`
	Indexes           map[string]map[string]*Index `yaml:"indexes"`
	Triggers          map[string]*Trigger          `yaml:"triggers"`
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
	fb := &FirebirdEngine{}
	var diffs []*DiffObject
	var columnDiffs []*DiffObject
	if !fb.Equals(t.Fields, other.Fields) {
		columnDiffs = append(diffs, fb.CompareScheme(t.Fields, other.Fields)...)
	}
	if len(columnDiffs) > 0 {
		for _, columnDiff := range columnDiffs {
			defColumns := t.DiffColumnDefinition(columnDiff, sep)
			definitions = append(definitions, defColumns...)
		}
	}
	for _, indexType := range IndexTypes() {
		if !fb.Equals(t.Indexes[indexType], other.Indexes[indexType]) {
			diffs = append(diffs, fb.CompareScheme(t.Indexes[indexType], other.Indexes[indexType])...)
		}
	}
	if !fb.Equals(t.Triggers, other.Triggers) {
		diffs = append(diffs, fb.CompareScheme(t.Triggers, other.Triggers)...)
	}
	for _, diff := range diffs {
		definitions = append(definitions, diff.DiffSql(sep)...)
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
	{{end}}{{ .Name }} {{if ne .Domain "" }}{{ .Domain }}{{ else }}{{ .Type }}{{end}}{{if ne .Charset "" }} CHARACTER SET {{ .Charset }}{{end}}{{if ne .Default "" }} {{ .Default }}{{end}}{{if .NotNull }} NOT NULL{{end}}{{if ne .Collate "" }} COLLATE {{ .Collate }}{{end}}{{end}}
)`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = tableTmpl.Execute(&tpl, t)
	if err != nil {
		return ""
	}

	return tpl.String()
}

func (t *Table) DiffColumnDefinition(diff *DiffObject, sep string) []string {
	var definitions []string
	switch diff.State {
	case DIFF_TYPE_CREATE:
		column := diff.To.(*TableColumn)
		definition := fmt.Sprintf("ALTER TABLE %s ADD %s", t.Name, column.Name)
		if column.Domain != "" {
			definition += " " + column.Domain
		} else {
			definition += " " + column.Type
			if column.Charset != "" {
				definition += " CHARACTER SET " + column.Charset
			}
			if column.Collate != "" {
				definition += " COLLATE " + column.Collate
			}
		}
		if column.NotNull {
			definition += " NOT NULL"
		}
		if column.Default != "" {
			definition += " DEFAULT " + column.Default
		}
		definitions = append(definitions, definition+sep+"\n")
		if column.Comment != "" {
			definitions = append(definitions, t.CommentOnColumn(column, sep))
		}
	case DIFF_TYPE_DROP:
		column := diff.From.(*TableColumn)
		definitions = append(definitions, fmt.Sprintf("ALTER TABLE %s DROP %s%s\n", t.Name, column.Name, sep))
	case DIFF_TYPE_UPDATE:
		columnFrom := diff.To.(*TableColumn)
		columnTo := diff.From.(*TableColumn)
		if columnTo.NotNull != columnFrom.NotNull {
			notnull := "NULL"
			if columnTo.NotNull {
				notnull = "1"
			}
			definitions = append(definitions, fmt.Sprintf("UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = %s WHERE RDB$FIELD_NAME = '%s' AND RDB$RELATION_NAME = '%s'%s\n", notnull, columnTo.Name, t.Name, sep))
		}
		if columnFrom.Domain != columnTo.Domain {
			if columnTo.Domain != "" {
				definitions = append(definitions, fmt.Sprintf("UPDATE RDB$RELATION_FIELDS SET RDB$FIELD_SOURCE = '%s' WHERE RDB$FIELD_NAME = '%s' AND RDB$RELATION_NAME = '%s'%s\n", columnTo.Domain, columnTo.Name, t.Name, sep))
			}
		} else if columnFrom.Type != columnTo.Type {
			definitions = append(definitions, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s%s\n", t.Name, columnTo.Name, columnTo.Type, sep))
		}
		if columnFrom.Default != columnTo.Default {
			definitions = append(definitions, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s%s\n", t.Name, columnTo.Name, columnTo.Default, sep))
		}
		if columnFrom.Charset != columnTo.Charset {
			definitions = append(definitions, fmt.Sprintf("UPDATE RDB$FIELDS SET RDB$CHARACTER_SET_ID = (SELECT FIRST 1 RDB$CHARACTER_SET_ID FROM RDB$COLLATIONS WHERE RDB$COLLATION_NAME = '%s') WHERE RDB$FIELD_NAME = '%s'%s\n", columnTo.Charset, columnFrom.FieldSource, sep))
		}
		if columnFrom.Collate != columnTo.Collate {
			definitions = append(definitions, fmt.Sprintf("UPDATE RDB$RELATION_FIELDS SET RDB$COLLATION_ID = (SELECT FIRST 1 RDB$COLLATION_ID FROM RDB$COLLATIONS WHERE RDB$COLLATION_NAME = '%s') WHERE RDB$FIELD_NAME = '%s' AND RDB$RELATION_NAME = '%s'%s\n", columnTo.Collate, columnFrom.Name, t.Name, sep))
		}
		if columnFrom.Comment != columnTo.Comment {
			definitions = append(definitions, t.CommentOnColumn(columnTo, sep))
		}
		if columnFrom.Position != columnTo.Position {
			definitions = append(definitions, fmt.Sprintf("ALTER TABLE %s ALTER %s POSITION %d%s\n", t.Name, columnTo.Name, columnTo.Position, sep))
		}
	}
	return definitions
}

func (t *Table) CommentOnColumn(column *TableColumn, sep string) string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'%s\n", t.Name, column.Name, column.Comment, sep)
}

func (t *Table) Equals(t2 interface{}) bool {
	other := t.CastType(t2)
	fb := &FirebirdEngine{}

	if !fb.Equals(t.Fields, other.Fields) {
		return false
	}

	for _, indexType := range IndexTypes() {
		if !fb.Equals(t.Indexes[indexType], other.Indexes[indexType]) {
			return false
		}
	}

	if !fb.Equals(t.Triggers, other.Triggers) {
		return false
	}

	return true
}

func (t *Table) Diff(t2 interface{}) *DiffObject {
	other := t.CastType(t2)

	if !t.Equals(other) {
		return &DiffObject{
			State:    DIFF_TYPE_UPDATE,
			Type:     t.GetTypeName(),
			From:     t,
			To:       other,
			Priority: TABLE_PRIORITY,
		}
	}

	return nil
}

func (t *Table) DiffsOnCreate(schema ElementSchema) []*DiffObject {
	var diffs []*DiffObject
	diffs = append(diffs, &DiffObject{
		State:    DIFF_TYPE_CREATE,
		Type:     t.GetTypeName(),
		From:     nil,
		To:       t,
		Priority: TABLE_PRIORITY,
	})

	fb := &FirebirdEngine{}
	for _, indexesByType := range t.Indexes {
		diffs = append(diffs, fb.CompareScheme(indexesByType, nil)...)
	}
	diffs = append(diffs, fb.CompareScheme(t.Triggers, nil)...)

	return diffs
}

func (t *Table) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return t.BaseElementSchema.DiffsOnDrop(schema)
}

func (t *Table) CastType(other interface{}) *Table {
	return other.(*Table)
}

func (t *Table) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	tablesMap := make(map[string]*Table)
	rows, err := conn.Query(`
		select trim(rdb$relation_name) 
		from rdb$relations
		where rdb$view_blr is null and 
		      (rdb$system_flag is null or rdb$system_flag = 0)
		order by rdb$relation_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		table := &Table{Fields: make(map[string]*TableColumn), Indexes: make(map[string]map[string]*Index), Triggers: make(map[string]*Trigger)}
		err := rows.Scan(&table.Name)
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
	var tables []ElementSchema
	for _, table := range tablesMap {
		tables = append(tables, table)
	}

	return tables, nil
}
