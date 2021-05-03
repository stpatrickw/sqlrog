package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"text/template"
)

type Trigger struct {
	sqlrog.BaseElementSchema `yaml:"base,omitempty"`
	Name                     string
	TableName                string
	TypeName                 string
	Source                   string
}

func (t *Trigger) GetTypeName() string {
	return "trigger"
}

func (t *Trigger) AlterDefinition(other interface{}, sep string) []string {
	return append(t.CastType(other).DropDefinition(sep), t.CastType(other).CreateDefinition(sep)...)
}

func (t *Trigger) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s%s", t.Definition(), sep)}
}

func (t *Trigger) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP TRIGGER IF EXISTS %s%s", t.Name, sep)}
}

func (t *Trigger) Definition() string {
	procTmpl, err := template.New("procedure").Parse(
		`TRIGGER {{ .Name }} {{ .TypeName}} ON {{ .TableName }} FOR EACH ROW
{{ .Source }}`)

	if err != nil {
		return ""
	}
	var tpl bytes.Buffer

	err = procTmpl.Execute(&tpl, t)
	if err != nil {
		return ""
	}

	return tpl.String()
}

func (t *Trigger) Equals(e2 interface{}) bool {
	other := t.CastType(e2)

	return t.Name == other.Name && t.TypeName == other.TypeName &&
		t.TableName == other.TableName && t.Source == other.Source
}

func (t *Trigger) Diff(t2 interface{}) *sqlrog.DiffObject {
	other := t.CastType(t2)

	if !t.Equals(other) {
		return &sqlrog.DiffObject{
			State: sqlrog.DIFF_TYPE_UPDATE,
			Type:  t.GetTypeName(),
			From:  t,
			To:    other,
		}
	}

	return nil
}

func (t *Trigger) CastType(other interface{}) *Trigger {
	return other.(*Trigger)
}

func (t *Trigger) FetchTriggersFromDB(conn *sql.DB) (map[string]map[string]*Trigger, error) {
	triggers := make(map[string]map[string]*Trigger)

	rows, err := conn.Query(`
		select EVENT_OBJECT_TABLE, TRIGGER_NAME, CONCAT(ACTION_TIMING, ' ', EVENT_MANIPULATION), ACTION_STATEMENT
		from information_schema.triggers
		where TRIGGER_SCHEMA = schema() `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		trigger := &Trigger{}
		err := rows.Scan(&trigger.TableName, &trigger.Name, &trigger.TypeName, &trigger.Source)
		if err != nil {
			return nil, err
		}
		if _, ok := triggers[trigger.TableName]; !ok {
			triggers[trigger.TableName] = make(map[string]*Trigger)
		}
		triggers[trigger.TableName][trigger.Name] = trigger
	}

	return triggers, nil
}
