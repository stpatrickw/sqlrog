package fb

import (
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
)

const (
	CORE_ELEMENT_ROLE_NAME        = "role"
	CORE_ELEMENT_ROLE_PLURAL_NAME = "roles"
)

type Role struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string `yaml:"name"`
}

func (r *Role) GetName() string {
	return r.Name
}

func (r *Role) GetTypeName() string {
	return CORE_ELEMENT_ROLE_NAME
}

func (r *Role) GetPluralTypeName() string {
	return CORE_ELEMENT_ROLE_PLURAL_NAME
}

func (r *Role) AlterDefinition(other interface{}, sep string) []string {
	return []string{}
}

func (r *Role) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s", r.Definition(sep))}
}

func (r *Role) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP %s", r.Definition(sep))}
}

func (r *Role) Definition(sep string) string {
	return fmt.Sprintf("ROLE %s%s", r.Name, sep)
}

func (r *Role) Equals(e2 interface{}) bool {
	other := r.CastType(e2)

	return r.Name == other.Name
}

func (r *Role) Diff(r2 interface{}) *sqlrog.DiffObject {
	other := r.CastType(r2)

	if !r.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  r.GetTypeName(),
			From:  r,
			To:    other,
		}
	}

	return nil
}

func (r *Role) CastType(other interface{}) *Role {
	return other.(*Role)
}

func (r *Role) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	var roles []sqlrog.ElementSchema

	rows, err := conn.Query(`SELECT trim(rdb$role_name) FROM RDB$ROLES WHERE rdb$system_flag = 0 order by 1`)
	if err != nil {
		return roles, err
	}
	defer rows.Close()
	for rows.Next() {
		role := &Role{}
		err := rows.Scan(&role.Name)
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}

	return roles, nil
}

func (r *Role) DiffsOnCreate(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return r.BaseElementSchema.DiffsOnCreate(schema)
}

func (r *Role) DiffsOnDrop(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return r.BaseElementSchema.DiffsOnDrop(schema)
}
