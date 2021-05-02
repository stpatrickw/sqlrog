package fb

import (
	"bytes"
	"database/sql"
	"fmt"
	. "github.com/stpatrickw/sqlrog/common"
	"text/template"
)

type Trigger struct {
	BaseElementSchema `yaml:"base,omitempty"`
	Name              string
	TableName         string
	TypeName          string
	Position          int
	Source            string
	Active            bool
}

func (t *Trigger) GetTypeName() string {
	return "trigger"
}

func (t *Trigger) AlterDefinition(other interface{}, sep string) []string {
	return []string{fmt.Sprintf("ALTER %s%s", t.CastType(other).Definition(), sep)}
}

func (t *Trigger) CreateDefinition(sep string) []string {
	return []string{fmt.Sprintf("CREATE %s%s", t.Definition(), sep)}
}

func (t *Trigger) DropDefinition(sep string) []string {
	return []string{fmt.Sprintf("DROP TRIGGER %s%s", t.Name, sep)}
}

func (t *Trigger) Definition() string {
	procTmpl, err := template.New("procedure").Parse(
		`TRIGGER {{ .Name }} FOR {{ .TableName }}
{{ if .Active }}ACTIVE{{ else }}INACTIVE{{end}} {{ .TypeName }} POSITION {{ .Position }}
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

	return t.Name == other.Name && t.TypeName == other.TypeName && t.TableName == other.TableName &&
		t.Position == other.Position && t.Source == other.Source && t.Active == other.Active
}

func (t *Trigger) Diff(t2 interface{}) *DiffObject {
	other := t.CastType(t2)

	if !t.Equals(other) {
		return &DiffObject{
			State: DIFF_TYPE_UPDATE,
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

	rows, err := conn.Query(`select
        trim(RDB$RELATION_NAME),
		trim(RDB$TRIGGER_NAME) as triggerName,
		case RDB$TRIGGER_INACTIVE when 1 then 0 else 1 end,
		trim(case RDB$TRIGGER_TYPE
			when 1 then  'before insert'
			when 2 then  'after insert'
			when 3 then  'before update'
			when 4 then  'after update'
			when 5 then  'before delete'
			when 6 then  'after delete'
			when 17 then  'before insert or update'
			when 18 then  'after insert or update'
			when 25 then  'before insert or delete'
			when 26 then  'after insert or delete'
			when 27 then  'before update or delete'
			when 28 then  'after update or delete'
			when 113 then  'before insert or update or delete'
			when 114 then  'after insert or update or delete'
			when 8192 then  'on connect'
			when 8193 then  'on disconnect'
			when 8194 then  'on transaction start'
			when 8195 then  'on transaction commit'
			when 8196 then  'on transaction rollback' end), 
		RDB$TRIGGER_SEQUENCE, RDB$TRIGGER_SOURCE
		from RDB$TRIGGERS where RDB$TRIGGER_SOURCE is not null AND RDB$SYSTEM_FLAG = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		trigger := &Trigger{}
		err := rows.Scan(&trigger.TableName, &trigger.Name, &trigger.Active, &trigger.TypeName, &trigger.Position, &trigger.Source)
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

func (t *Trigger) DiffsOnCreate(schema ElementSchema) []*DiffObject {
	return t.BaseElementSchema.DiffsOnCreate(schema)
}

func (t *Trigger) DiffsOnDrop(schema ElementSchema) []*DiffObject {
	return t.BaseElementSchema.DiffsOnDrop(schema)
}
