package fb

import (
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
)

const (
	CORE_ELEMENT_GENERATOR_NAME        = "generator"
	CORE_ELEMENT_GENERATOR_PLURAL_NAME = "generators"
)

type Generator struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string `yaml:"name"`
	Comment                  string `yaml:"comment"`
}

func (g *Generator) GetName() string {
	return g.Name
}

func (g *Generator) GetTypeName() string {
	return CORE_ELEMENT_GENERATOR_NAME
}

func (g *Generator) GetPluralTypeName() string {
	return CORE_ELEMENT_GENERATOR_PLURAL_NAME
}

func (g *Generator) AlterDefinition(other interface{}, sep string) []string {
	definitions := []string{fmt.Sprintf("ALTER %s", g.CastType(other).Definition(sep))}
	if comment := g.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (g *Generator) CreateDefinition(sep string) []string {
	definitions := []string{fmt.Sprintf("CREATE %s", g.Definition(sep))}
	if comment := g.AddComment(sep); comment != "" {
		definitions = append(definitions, comment)
	}
	return definitions
}

func (g *Generator) DropDefinition(sep string) []string {
	g.Comment = ""
	return []string{fmt.Sprintf("DROP %s", g.Definition(sep))}
}

func (g *Generator) Definition(sep string) string {
	SQL := fmt.Sprintf("SEQUENCE %s%s\n", g.Name, sep)
	if g.Comment != "" {
		SQL += fmt.Sprintf("COMMENT ON SEQUENCE %s IS '%s'\n", g.Name, g.Comment)
	}
	return SQL
}

func (g *Generator) AddComment(sep string) string {
	if g.Comment != "" {
		return fmt.Sprintf("COMMENT ON SEQUENCE %s IS '%s'%s", g.Name, g.Comment, sep)
	}
	return ""
}

func (g *Generator) Equals(g2 interface{}) bool {
	other := g.CastType(g2)

	return g.Name == other.Name && g.Comment == other.Comment
}

func (g *Generator) Diff(e2 interface{}) *sqlrog.DiffObject {
	other := g.CastType(e2)

	if !g.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  g.GetTypeName(),
			From:  g,
			To:    other,
		}
	}

	return nil
}

func (g *Generator) CastType(other interface{}) *Generator {
	return other.(*Generator)
}

func (g *Generator) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	var generators []sqlrog.ElementSchema

	rows, err := conn.Query(`
		select trim(rdb$generator_name), trim(coalesce(rdb$description, ''))
		from rdb$generators
		where rdb$system_flag = 0
		order by 1`)
	if err != nil {
		return generators, err
	}
	defer rows.Close()
	for rows.Next() {
		generator := &Generator{}
		err := rows.Scan(&generator.Name, &generator.Comment)
		if err != nil {
			return nil, err
		}
		generators = append(generators, generator)
	}

	return generators, nil
}

func (g *Generator) DiffsOnCreate(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return g.BaseElementSchema.DiffsOnCreate(schema)
}

func (g *Generator) DiffsOnDrop(schema sqlrog.ElementSchema) []*sqlrog.DiffObject {
	return g.BaseElementSchema.DiffsOnDrop(schema)
}
