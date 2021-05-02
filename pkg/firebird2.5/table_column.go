package fb

import (
	"database/sql"
	. "github.com/stpatrickw/sqlrog/common"
)

type TableColumn struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string
	Type              string
	Domain            string
	FieldSource       string
	NotNull           bool
	Charset           string
	Collate           string
	Default           string
	Comment           string
	Position          int
}

func (t *TableColumn) Equals(t2 interface{}) bool {
	other := t.CastType(t2)

	return t.Type == other.Type && t.Domain == other.Domain && t.NotNull == other.NotNull &&
		t.Charset == other.Charset && t.Collate == other.Collate && t.Default == other.Default &&
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
			SELECT
               TRIM(RF.RDB$RELATION_NAME),
              TRIM(RF.RDB$FIELD_NAME) FIELD_NAME, TRIM(RF.RDB$FIELD_SOURCE),
              TRIM(COALESCE((CASE F.RDB$FIELD_TYPE
                WHEN 7 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'SMALLINT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 8 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'INTEGER'
                    WHEN 1 THEN 'NUMERIC('  || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 9 THEN 'QUAD'
                WHEN 10 THEN 'FLOAT'
                WHEN 12 THEN 'DATE'
                WHEN 13 THEN 'TIME'
                WHEN 14 THEN 'CHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ') '
                WHEN 16 THEN
                  CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'BIGINT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                  END
                WHEN 27 THEN 'DOUBLE'
                WHEN 35 THEN 'TIMESTAMP'
                WHEN 37 THEN 'VARCHAR(' || F.RDB$FIELD_LENGTH || ')'
                WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
                WHEN 45 THEN 'BLOB_ID'
                WHEN 261 THEN 'BLOB SUB_TYPE ' || F.RDB$FIELD_SUB_TYPE
                ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'
              END), '')) FIELD_TYPE,
              TRIM(CASE WHEN not (rf.rdb$field_source starting with 'RDB$') THEN rf.rdb$field_source ELSE '' END) AS DOMAIN_NAME,
              COALESCE(RF.RDB$NULL_FLAG, 0) FIELD_NULL,
              TRIM(COALESCE(NULLIF(CH.RDB$CHARACTER_SET_NAME, 'NONE'), '')) FIELD_CHARSET,
              TRIM(COALESCE(NULLIF(DCO.RDB$COLLATION_NAME, 'NONE'), '')) FIELD_COLLATION,
              TRIM(COALESCE(RF.RDB$DEFAULT_SOURCE, F.RDB$DEFAULT_SOURCE, '')) FIELD_DEFAULT,
            --  F.RDB$VALIDATION_SOURCE FIELD_CHECK,
              TRIM(COALESCE(RF.RDB$DESCRIPTION, '')) FIELD_DESCRIPTION,
              RF.RDB$FIELD_POSITION +1 
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$RELATIONS R ON R.RDB$RELATION_NAME = RF.RDB$RELATION_NAME
            JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)
            LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
            LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = RF.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))
            WHERE COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0 AND R.rdb$view_blr is null
            ORDER BY RF.RDB$FIELD_POSITION`)
	if err != nil {
		return nil, err
	}
	defer fieldRows.Close()

	for fieldRows.Next() {
		field := &TableColumn{}
		var relationName string
		err := fieldRows.Scan(&relationName, &field.Name, &field.FieldSource, &field.Type, &field.Domain, &field.NotNull, &field.Charset, &field.Collate, &field.Default, &field.Comment, &field.Position)
		if err != nil {
			return nil, err
		}
		if _, ok := fields[relationName]; !ok {
			fields[relationName] = make(map[string]*TableColumn)
		}
		fields[relationName][field.Name] = field
	}

	return fields, nil
}
