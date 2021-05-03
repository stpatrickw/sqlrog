package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"text/template"
)

const (
	CORE_ELEMENT_PROCEDURE_NAME        = "procedure"
	CORE_ELEMENT_PROCEDURE_PLURAL_NAME = "procedures"
)

type Procedure struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string                         `yaml:"name"`
	Source                   string                         `yaml:"source"`
	InputParameters          map[string]*ProcedureParameter `yaml:"input_params"`
	OutputParameters         map[string]*ProcedureParameter `yaml:"output_params"`
	Deterministic            bool
}

type ProcedureParameter struct {
	Name     string
	TypeName string
	Charset  string
	Collate  string
	Position int
}

func (p *Procedure) GetName() string {
	return p.Name
}

func (p *Procedure) GetTypeName() string {
	return CORE_ELEMENT_PROCEDURE_NAME
}

func (p *Procedure) GetPluralTypeName() string {
	return CORE_ELEMENT_PROCEDURE_PLURAL_NAME
}

func (p *Procedure) AlterDefinition(other interface{}, sep string) []string {
	return append(p.CastType(other).DropDefinition(sep), p.CastType(other).CreateDefinition(sep)...)
}

func (p *Procedure) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s", p.Definition(sep))}
}

func (p *Procedure) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP PROCEDURE IF EXISTS %s%s", p.Name, sep)}
}

func (p *Procedure) Definition(sep string) string {
	procTmpl, err := template.New("procedure").Parse(`PROCEDURE ` + "{{ .Name}}" + `({{if .InputParameters}}
	{{$first := true}}{{range $index, $element := .InputParameters}}{{if $first}}{{$first = false}}{{else}},
	{{end}}IN {{.Name}} {{.TypeName}}{{if ne .Charset ""}} CHARACTER SET {{.Charset}}{{end}}{{if ne .Collate ""}} COLLATE {{.Collate}}{{end}}{{end}}{{end}}{{if .OutputParameters}}{{if .InputParameters}},{{end}}
	{{$first := true}}{{range $index, $element := .OutputParameters}}{{if $first}}{{$first = false}}{{else}},
	{{end}}OUT {{.Name}} {{.TypeName}}{{if ne .Charset ""}} CHARACTER SET {{.Charset}}{{end}}{{if ne .Collate ""}} COLLATE {{.Collate}}{{end}}{{end}}{{end}}){{if .Deterministic}} DETERMINISTIC{{end}}
{{ .Source }}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = procTmpl.Execute(&tpl, p)
	if err != nil {
		return ""
	}

	return tpl.String() + sep
}

func (p *Procedure) Equals(e2 interface{}) bool {
	other := p.CastType(e2)

	if p.Name != other.Name || p.Source != other.Source || p.Deterministic != other.Deterministic {
		return false
	}

	if !ProcedureParamsEquals(p.InputParameters, other.InputParameters) {
		return false
	}

	if !ProcedureParamsEquals(p.OutputParameters, other.OutputParameters) {
		return false
	}

	return true
}

func (p *Procedure) Diff(e2 interface{}) *sqlrog.DiffObject {
	other := p.CastType(e2)

	if !p.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  p.GetTypeName(),
			From:  p,
			To:    other,
		}
	}

	return nil
}

func ProcedureParamsEquals(src map[string]*ProcedureParameter, dest map[string]*ProcedureParameter) bool {
	if len(src) != len(dest) {
		return false
	}
	for name, param := range src {
		if _, ok := dest[name]; !ok {
			return false
		}
		if !ParamEquals(param, dest[name]) {
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

func ParamEquals(src *ProcedureParameter, dest *ProcedureParameter) bool {
	return src.Name == dest.Name && src.Position == dest.Position && src.TypeName == dest.TypeName &&
		src.Charset == dest.Charset && src.Collate == dest.Collate
}

func (p *Procedure) CastType(other interface{}) *Procedure {
	return other.(*Procedure)
}

func (p *Procedure) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	var procedures []sqlrog.ElementSchema

	rows, err := conn.Query(`SELECT SPECIFIC_NAME, ROUTINE_DEFINITION, IS_DETERMINISTIC 
		FROM information_schema.routines WHERE routine_schema = schema() and routine_type = 'PROCEDURE' order by 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	parametersQuery := `SELECT SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DTD_IDENTIFIER, CHARACTER_SET_NAME, COLLATION_NAME, ORDINAL_POSITION 
		FROM information_schema.parameters
		WHERE SPECIFIC_SCHEMA = schema() AND ROUTINE_TYPE = 'PROCEDURE';`

	parameterRows, err := conn.Query(parametersQuery)
	if err != nil {
		return procedures, err
	}
	inputParams := make(map[string]map[string]*ProcedureParameter)
	outputParams := make(map[string]map[string]*ProcedureParameter)
	for parameterRows.Next() {
		var paramType string
		var procedureName string
		procedureParam := &ProcedureParameter{}
		err := parameterRows.Scan(&procedureName, &procedureParam.Name, &paramType, &procedureParam.TypeName, &procedureParam.Charset, &procedureParam.Collate, &procedureParam.Position)
		if err != nil {
			return nil, err
		}

		if paramType == "IN" {
			if _, ok := inputParams[procedureName]; !ok {
				inputParams[procedureName] = make(map[string]*ProcedureParameter)
			}
			inputParams[procedureName][procedureParam.Name] = procedureParam
		} else if paramType == "OUT" {
			if _, ok := outputParams[procedureName]; !ok {
				outputParams[procedureName] = make(map[string]*ProcedureParameter)
			}
			outputParams[procedureName][procedureParam.Name] = procedureParam
		}
	}
	parameterRows.Close()
	for rows.Next() {
		var deterministic string
		procedure := &Procedure{InputParameters: make(map[string]*ProcedureParameter), OutputParameters: make(map[string]*ProcedureParameter)}
		err := rows.Scan(&procedure.Name, &procedure.Source, &deterministic)
		if err != nil {
			return nil, err
		}
		if deterministic == "YES" {
			procedure.Deterministic = true
		}
		if _, ok := inputParams[procedure.Name]; ok {
			procedure.InputParameters = inputParams[procedure.Name]
		}
		if _, ok := outputParams[procedure.Name]; ok {
			procedure.OutputParameters = outputParams[procedure.Name]
		}
		procedures = append(procedures, procedure)
	}

	return procedures, nil
}
