package fb

import (
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
)

const (
	CORE_ELEMENT_DOMAIN_NAME        = "domain"
	CORE_ELEMENT_DOMAIN_PLURAL_NAME = "domains"
)

type Domain struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string `yaml:"name"`
	Type              string `yaml:"type"`
	Default           string `yaml:"default"`
	Notnull           bool   `yaml:"notnull"`
	Comment           string `yaml:"comment"`
}

func (d *Domain) GetName() string {
	return d.Name
}

func (d *Domain) GetTypeName() string {
	return CORE_ELEMENT_DOMAIN_NAME
}

func (d *Domain) GetPluralTypeName() string {
	return CORE_ELEMENT_DOMAIN_PLURAL_NAME
}

func (d *Domain) AlterDefinition(other interface{}, sep string) []string {
	definitions := []string{fmt.Sprintf("ALTER %s", d.CastType(other).Definition(sep))}
	if comment := d.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (d *Domain) CreateDefinition(sep string) []string {
	definitions := []string{fmt.Sprintf("CREATE %s", d.Definition(sep))}
	if comment := d.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (d *Domain) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP DOMAIN %s%s", d.Name, sep)}
}

func (d *Domain) Definition(sep string) string {
	SQL := fmt.Sprintf("DOMAIN %s AS %s", d.Name, d.Type)
	if d.Default != "" {
		SQL += " " + d.Default
	}
	if d.Notnull {
		SQL += " NOT NULL"
	}
	SQL += sep

	return SQL
}

func (d *Domain) DiffsOnCreate(schema ElementSchema) []*DiffObject {
	return d.BaseElementSchema.DiffsOnCreate(schema)
}

func (d *Domain) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return d.BaseElementSchema.DiffsOnDrop(schema)
}

func (d *Domain) AddComment(sep string) string {
	if d.Comment != "" {
		return fmt.Sprintf("COMMENT ON DOMAIN %s IS '%s'%s", d.Name, d.Comment, sep)
	}
	return ""
}

func (d *Domain) Equals(d2 interface{}) bool {
	other := d.CastType(d2)

	return d.Name == other.Name && d.Type == other.Type && d.Notnull == other.Notnull &&
		d.Default == other.Default && d.Comment == other.Comment
}

func (d *Domain) Diff(d2 interface{}) *DiffObject {
	other := d.CastType(d2)

	if !d.Equals(other) {
		return &DiffObject{
			State: DIFF_TYPE_UPDATE,
			Type:  d.GetTypeName(),
			From:  d,
			To:    other,
		}
	}

	return nil
}

func (d *Domain) CastType(other interface{}) *Domain {
	return other.(*Domain)
}

func (d *Domain) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	var domains []ElementSchema

	rows, err := conn.Query(`
		select
		 trim(F.RDB$FIELD_NAME),
		 trim(CASE F.RDB$FIELD_TYPE
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
				WHEN 37 THEN 'VARCHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
				WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
				WHEN 45 THEN 'BLOB_ID'
				WHEN 261 THEN 'BLOB SUB_TYPE ' || F.RDB$FIELD_SUB_TYPE
				ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'
			end),
		trim(coalesce(F.rdb$default_source, '')),
		coalesce(F.rdb$null_flag,0),
		trim(coalesce(F.rdb$description, ''))
		FROM RDB$FIELDS F
		LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
		WHERE COALESCE( F.rdb$system_flag, 0) = 0 AND NOT ( F.rdb$field_name STARTING WITH 'RDB$')
		order by 1`)
	if err != nil {
		return domains, err
	}
	defer rows.Close()
	for rows.Next() {
		domain := &Domain{}
		err := rows.Scan(&domain.Name, &domain.Type, &domain.Default, &domain.Notnull, &domain.Comment)
		if err != nil {
			return nil, err
		}
		domains = append(domains, domain)
	}

	return domains, nil
}
