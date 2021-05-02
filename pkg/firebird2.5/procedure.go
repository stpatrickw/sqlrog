package fb

import (
	"bytes"
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
	"text/template"
)

const (
	CORE_ELEMENT_PROCEDURE_NAME        = "procedure"
	CORE_ELEMENT_PROCEDURE_PLURAL_NAME = "procedures"
)

type Procedure struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string                         `yaml:"name"`
	Source            string                         `yaml:"source"`
	InputParameters   map[string]*ProcedureParameter `yaml:"input_params"`
	OutputParameters  map[string]*ProcedureParameter `yaml:"output_params"`
}

type ProcedureParameter struct {
	Name     string
	TypeName string
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
	return []string{fmt.Sprintf("ALTER %s", p.CastType(other).Definition(sep))}
}

func (p *Procedure) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s", p.Definition(sep))}
}

func (p *Procedure) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP PROCEDURE %s%s", p.Name, sep)}
}

func (p *Procedure) Definition(sep string) string {
	procTmpl, err := template.New("procedure").Parse(`PROCEDURE {{ .Name}} {{if .InputParameters}}(
	{{$first := true}}{{range $index, $element := .InputParameters}}{{if $first}}{{$first = false}}{{else}},
	{{end}}{{.Name}} {{.TypeName}}{{end}}) {{end}}{{if .OutputParameters}}
returns (
	{{$first := true}}{{range $index, $element := .OutputParameters}}{{if $first}}{{$first = false}}{{else}},
	{{end}}{{.Name}} {{.TypeName}}{{end}}){{end}}
as
{{ .Source }}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = procTmpl.Execute(&tpl, p)
	if err != nil {
		return ""
	}

	return tpl.String() + sep + "\n"
}

func (p *Procedure) Equals(e2 interface{}) bool {
	other := p.CastType(e2)

	if p.Name != other.Name || p.Source != other.Source {
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

func (p *Procedure) Diff(e2 interface{}) *DiffObject {
	other := p.CastType(e2)

	if !p.Equals(other) {
		return &DiffObject{
			State: DIFF_TYPE_UPDATE,
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
	return src.Name == dest.Name && src.Position == dest.Position && src.TypeName == dest.TypeName
}

func (p *Procedure) CastType(other interface{}) *Procedure {
	return other.(*Procedure)
}

func (p *Procedure) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	var procedures []ElementSchema

	rows, err := conn.Query(`
		select trim(rdb$procedure_name), rdb$procedure_source
		from rdb$procedures order by 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	parametersQuery := `SELECT
       	      TRIM(rdb$procedure_name),
       	      rdb$parameter_type,
              TRIM(RF.RDB$PARAMETER_NAME),
              TRIM(CASE WHEN not (rf.rdb$field_source starting with 'RDB$') THEN rf.rdb$field_source ELSE
               CASE F.RDB$FIELD_TYPE
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
              END END) FIELD_TYPE,
              RF.RDB$PARAMETER_NUMBER
            FROM RDB$PROCEDURE_PARAMETERS RF
            JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)
            LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
            LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = F.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))
            WHERE COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0
            ORDER BY RF.RDB$PARAMETER_NUMBER`

	parameterRows, err := conn.Query(parametersQuery)
	if err != nil {
		return procedures, err
	}
	inputParams := make(map[string]map[string]*ProcedureParameter)
	outputParams := make(map[string]map[string]*ProcedureParameter)
	for parameterRows.Next() {
		var paramType int
		var procedureName string
		procedureParam := &ProcedureParameter{}
		err := parameterRows.Scan(&procedureName, &paramType, &procedureParam.Name, &procedureParam.TypeName, &procedureParam.Position)
		if err != nil {
			return nil, err
		}

		if paramType == 0 {
			if _, ok := inputParams[procedureName]; !ok {
				inputParams[procedureName] = make(map[string]*ProcedureParameter)
			}
			inputParams[procedureName][procedureParam.Name] = procedureParam
		} else if paramType == 1 {
			if _, ok := outputParams[procedureName]; !ok {
				outputParams[procedureName] = make(map[string]*ProcedureParameter)
			}
			outputParams[procedureName][procedureParam.Name] = procedureParam
		}
	}
	parameterRows.Close()
	for rows.Next() {
		procedure := &Procedure{InputParameters: make(map[string]*ProcedureParameter), OutputParameters: make(map[string]*ProcedureParameter)}
		err := rows.Scan(&procedure.Name, &procedure.Source)
		if err != nil {
			return nil, err
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

func (p *Procedure) DiffsOnCreate(schema ElementSchema) []*DiffObject {
	return p.BaseElementSchema.DiffsOnCreate(schema)
}

func (p *Procedure) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return p.BaseElementSchema.DiffsOnDrop(schema)
}
