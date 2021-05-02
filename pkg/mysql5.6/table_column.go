package mysql

import (
	"database/sql"
	. "github.com/stpatrickw/sqlrog/common"
)

type TableColumn struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string
	Type              string
	NotNull           bool
	Charset           string
	Collate           string
	UseDefault        bool
	Default           string
	Key               string
	Extra             string
	Comment           string
	Position          int
}

func (t *TableColumn) Equals(t2 interface{}) bool {
	other := t.CastType(t2)

	return t.Type == other.Type && t.UseDefault == other.UseDefault && t.Key == other.Key && t.NotNull == other.NotNull &&
		t.Extra == other.Extra && t.Charset == other.Charset && t.Collate == other.Collate && t.Default == other.Default &&
		t.Comment == other.Comment && t.Position == other.Position
}

func (f *TableColumn) Diff(t2 interface{}) *DiffObject {
	other := f.CastType(t2)

	if !f.Equals(other) {
		return &DiffObject{
			State: DIFF_TYPE_UPDATE,
			Type:  f.GetTypeName(),
			From:  f,
			To:    other,
		}
	}

	return nil
}

func (f *TableColumn) CastType(other interface{}) *TableColumn {
	return other.(*TableColumn)
}

func (f *TableColumn) GetTypeName() string {
	return "table_column"
}

func (f *TableColumn) FetchColumnsFromDB(conn *sql.DB) (map[string]map[string]*TableColumn, error) {
	fields := make(map[string]map[string]*TableColumn)

	fieldRows, err := conn.Query(`
			SELECT t.table_name, c.column_name, c.column_type, c.is_nullable, 
				case when c.column_default is null then 0 else 1 end, coalesce(c.column_default, ''),
           		c.column_key, c.extra, coalesce(c.character_set_name, ''), coalesce(c.collation_name, ''), c.column_comment, c.ordinal_position
			FROM INFORMATION_SCHEMA.TABLES t 
			JOIN INFORMATION_SCHEMA.COLUMNS c ON t.table_schema = c.table_schema and t.table_name = c.table_name
			WHERE t.table_schema = schema() AND t.TABLE_TYPE = 'BASE TABLE' ORDER BY c.ordinal_position`)
	if err != nil {
		return nil, err
	}
	defer fieldRows.Close()

	for fieldRows.Next() {
		field := &TableColumn{}
		var (
			relationName string
			nullable     string
			useDefault   int
		)
		err := fieldRows.Scan(&relationName, &field.Name, &field.Type, &nullable, &useDefault, &field.Default, &field.Key, &field.Extra, &field.Charset, &field.Collate, &field.Comment, &field.Position)
		if err != nil {
			return nil, err
		}
		if nullable == "NO" {
			field.NotNull = true
		}
		if useDefault == 1 {
			field.UseDefault = true
		}
		if _, ok := fields[relationName]; !ok {
			fields[relationName] = make(map[string]*TableColumn)
		}
		fields[relationName][field.Name] = field
	}

	return fields, nil
}
