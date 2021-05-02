package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
	"text/template"
)

const (
	CORE_ELEMENT_VIEW_NAME        = "view"
	CORE_ELEMENT_VIEW_PLURAL_NAME = "views"
)

type View struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string `yaml:"name"`
	Source            string `yaml:"source"`
}

func (v *View) GetName() string {
	return v.Name
}

func (v *View) GetTypeName() string {
	return CORE_ELEMENT_VIEW_NAME
}

func (v *View) GetPluralTypeName() string {
	return CORE_ELEMENT_VIEW_PLURAL_NAME
}

func (v *View) AlterDefinition(other interface{}, sep string) []string {
	return []string{v.Definition(sep)}
}

func (v *View) CreateDefinition(sep string) []string {
	return []string{v.Definition(sep)}
}

func (v *View) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP VIEW %s%s", v.Name, sep)}
}

func (v *View) Definition(sep string) string {
	procTmpl, err := template.New("view").Parse(`CREATE OR REPLACE VIEW {{ .Name}} 
as {{ .Source }}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = procTmpl.Execute(&tpl, v)
	if err != nil {
		return ""
	}

	return tpl.String() + sep
}

func (p *View) Equals(e2 interface{}) bool {
	other := p.CastType(e2)

	if p.Name != other.Name || p.Source != other.Source {
		return false
	}

	return true
}

func (v *View) Diff(e2 interface{}) *DiffObject {
	other := v.CastType(e2)

	if !v.Equals(other) {
		return &DiffObject{
			State: DIFF_TYPE_UPDATE,
			Type:  v.GetTypeName(),
			From:  v,
			To:    other,
		}
	}

	return nil
}

func (v *View) CastType(other interface{}) *View {
	return other.(*View)
}

func (v *View) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	var views []ElementSchema

	rows, err := conn.Query(`
		SELECT TABLE_NAME, VIEW_DEFINITION 
		from INFORMATION_SCHEMA.VIEWS 
		where TABLE_SCHEMA = schema() 
		order by 1`)
	if err != nil {
		return views, err
	}
	defer rows.Close()
	for rows.Next() {
		view := &View{}
		err := rows.Scan(&view.Name, &view.Source)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}

	return views, nil
}