package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"text/template"
)

const (
	CORE_ELEMENT_FUNCTION_NAME        = "function"
	CORE_ELEMENT_FUNCTION_PLURAL_NAME = "functions"
)

type Function struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string                        `yaml:"name"`
	Source                   string                        `yaml:"source"`
	InputParameters          map[string]*FunctionParameter `yaml:"input_params"`
	OutputParameterType      string                        `yaml:"output_parameter_type"`
	OutputParameterCharset   string                        `yaml:"output_parameter_charset"`
	Deterministic            bool
}

type FunctionParameter struct {
	Name     string
	TypeName string
	Charset  string
	Collate  string
	Position int
}

func (f *Function) GetName() string {
	return f.Name
}

func (f *Function) GetTypeName() string {
	return CORE_ELEMENT_FUNCTION_NAME
}

func (f *Function) GetPluralTypeName() string {
	return CORE_ELEMENT_FUNCTION_PLURAL_NAME
}

func (f *Function) AlterDefinition(other interface{}, sep string) []string {
	return append(f.CastType(other).DropDefinition(sep), f.CastType(other).CreateDefinition(sep)...)
}

func (f *Function) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s", f.Definition(sep))}
}

func (f *Function) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP FUNCTION IF EXISTS %s%s", f.Name, sep)}
}

func (f *Function) Definition(sep string) string {
	procTmpl, err := template.New("function").Parse(`FUNCTION ` + "{{ .Name}}" + `({{if .InputParameters}}
	{{$first := true}}{{range $index, $element := .InputParameters}}{{if $first}}{{$first = false}}{{else}},
	{{end}}{{.Name}} {{.TypeName}}{{if ne .Charset "" }} CHARSET {{.Charset}}{{end}}{{end}}{{end}}) RETURNS {{ .OutputParameterType}}{{if ne .OutputParameterCharset "" }} CHARSET {{ .OutputParameterCharset}}{{end}}{{if .Deterministic}} DETERMINISTIC{{end}}
{{ .Source }}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = procTmpl.Execute(&tpl, f)
	if err != nil {
		return ""
	}

	return tpl.String() + sep
}

func (f *Function) Equals(e2 interface{}) bool {
	other := f.CastType(e2)

	if f.Name != other.Name || f.Source != other.Source || f.Deterministic != other.Deterministic ||
		f.OutputParameterType != other.OutputParameterType || f.OutputParameterCharset != other.OutputParameterCharset {
		return false
	}

	if !FunctionParamsEquals(f.InputParameters, other.InputParameters) {
		return false
	}

	return true
}

func (f *Function) Diff(e2 interface{}) *sqlrog.DiffObject {
	other := f.CastType(e2)

	if !f.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  f.GetTypeName(),
			From:  f,
			To:    other,
		}
	}

	return nil
}

func FunctionParamsEquals(src map[string]*FunctionParameter, dest map[string]*FunctionParameter) bool {
	if len(src) != len(dest) {
		return false
	}
	for name, param := range src {
		if _, ok := dest[name]; !ok {
			return false
		}
		if !FunctionParamEquals(param, dest[name]) {
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

func FunctionParamEquals(src *FunctionParameter, dest *FunctionParameter) bool {
	return src.Name == dest.Name && src.Position == dest.Position && src.TypeName == dest.TypeName && src.Charset == dest.Charset
}

func (f *Function) CastType(other interface{}) *Function {
	return other.(*Function)
}

func (f *Function) FetchElementsFromDB(conn *sql.DB) ([]sqlrog.ElementSchema, error) {
	var functions []sqlrog.ElementSchema

	rows, err := conn.Query(`SELECT r.SPECIFIC_NAME, r.ROUTINE_DEFINITION, p.DTD_IDENTIFIER, coalesce(p.CHARACTER_SET_NAME,''), r.IS_DETERMINISTIC
		FROM information_schema.routines r
		JOIN information_schema.parameters p on p.specific_name = r.specific_name and p.parameter_mode is null
		WHERE r.ROUTINE_SCHEMA = schema() AND r.routine_type = 'FUNCTION' order by 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	parametersQuery := `SELECT SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DTD_IDENTIFIER, ORDINAL_POSITION, coalesce(CHARACTER_SET_NAME,'')
		FROM information_schema.parameters
		WHERE SPECIFIC_SCHEMA = schema() AND ROUTINE_TYPE = 'FUNCTION' AND PARAMETER_MODE = 'IN'`

	parameterRows, err := conn.Query(parametersQuery)
	if err != nil {
		return nil, err
	}
	inputParams := make(map[string]map[string]*FunctionParameter)
	for parameterRows.Next() {
		var (
			paramType    string
			functionName string
		)
		functionParam := &FunctionParameter{}
		err := parameterRows.Scan(&functionName, &functionParam.Name, &paramType, &functionParam.TypeName, &functionParam.Position, &functionParam.Charset)
		if err != nil {
			return nil, err
		}
		if _, ok := inputParams[functionName]; !ok {
			inputParams[functionName] = make(map[string]*FunctionParameter)
		}
		inputParams[functionName][functionParam.Name] = functionParam
	}
	parameterRows.Close()
	for rows.Next() {
		var deterministic string
		function := &Function{InputParameters: make(map[string]*FunctionParameter)}
		err := rows.Scan(&function.Name, &function.Source, &function.OutputParameterType, &function.OutputParameterCharset, &deterministic)
		if err != nil {
			return nil, err
		}
		if deterministic == "YES" {
			function.Deterministic = true
		}
		if _, ok := inputParams[function.Name]; ok {
			function.InputParameters = inputParams[function.Name]
		}
		functions = append(functions, function)
	}

	return functions, nil
}
