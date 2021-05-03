package fb

import (
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
)

const (
	CORE_ELEMENT_EXCEPTION_NAME        = "exception"
	CORE_ELEMENT_EXCEPTION_PLURAL_NAME = "exceptions"
)

type Exception struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string `yaml:"name"`
	Number                   int    `yaml:"number"`
	Message                  string `yaml:"message"`
	Comment                  string `yaml:"comment"`
}

func (e *Exception) GetName() string {
	return e.Name
}

func (e *Exception) GetTypeName() string {
	return CORE_ELEMENT_EXCEPTION_NAME
}

func (e *Exception) GetPluralTypeName() string {
	return CORE_ELEMENT_EXCEPTION_PLURAL_NAME
}

func (e *Exception) AlterDefinition(other interface{}, sep string) []string {
	definitions := []string{fmt.Sprintf("ALTER %s", e.CastType(other).Definition(sep))}
	if comment := e.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (e *Exception) CreateDefinition(sep string) []string {
	definitions := []string{fmt.Sprintf("CREATE %s", e.Definition(sep))}
	if comment := e.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (e *Exception) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP EXCEPTION %s%s", e.Name, sep)}
}

func (e *Exception) Definition(sep string) string {
	return fmt.Sprintf("EXCEPTION %s '%s'%s", e.Name, e.Message, sep)
}

func (e *Exception) AddComment(sep string) string {
	if e.Comment != "" {
		return fmt.Sprintf("COMMENT ON EXCEPTION %s IS '%s'%s", e.Name, e.Comment, sep)
	}
	return ""
}

func (e *Exception) Equals(e2 interface{}) bool {
	other := e.CastType(e2)

	return e.Name == other.Name && e.Comment == other.Comment && e.Message == other.Message
}

func (e *Exception) Diff(e2 interface{}) *sqlrog.DiffObject {
	other := e.CastType(e2)

	if !e.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  e.GetTypeName(),
			From:  e,
			To:    other,
		}
	}

	return nil
}

func (e *Exception) CastType(other interface{}) *Exception {
	return other.(*Exception)
}

func (e *Exception) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	var exceptions []sqlrog.ElementSchema
	exRows, err := conn.Query(`
		select trim(ex.rdb$exception_name), ex.rdb$exception_number, trim(coalesce(ex.rdb$message, '')), trim(coalesce(ex.rdb$description, ''))
		from rdb$exceptions ex
		where rdb$system_flag = 0
		order by 1`)
	if err != nil {
		return exceptions, err
	}
	defer exRows.Close()
	for exRows.Next() {
		exception := &Exception{}
		err := exRows.Scan(&exception.Name, &exception.Number, &exception.Message, &exception.Comment)
		if err != nil {
			return nil, err
		}
		exceptions = append(exceptions, exception)
	}

	return exceptions, nil
}

func (e *Exception) DiffsOnCreate(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return e.BaseElementSchema.DiffsOnCreate(schema)
}

func (e *Exception) DiffsOnDrop(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return e.BaseElementSchema.DiffsOnDrop(schema)
}
