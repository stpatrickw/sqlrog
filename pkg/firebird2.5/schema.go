package fb

import (
	"database/sql"
	"fmt"
	_ "github.com/nakagami/firebirdsql"
	"github.com/pkg/errors"
	. "github.com/stpatrickw/sqlrog/common"
	"os"
	"reflect"
)

type FirebirdEngine struct {
	CoreEngine
}

func init() {
	fb := &FirebirdEngine{
		CoreEngine{
			Name:  "Firebird",
			Alias: "fb2.5",
		},
	}
	Engines[fb.Alias] = fb
}

type FbParams struct {
	Host     string `yaml:"host" validate:"required"`
	Port     string `yaml:"port" validate:"required"`
	Database string `yaml:"database" validate:"required"`
	User     string `yaml:"user" validate:"required"`
	Password string `yaml:"password" validate:"required"`
}

func (params *FbParams) GetParam(key string) string {
	r := reflect.ValueOf(params)
	f := reflect.Indirect(r).FieldByName(key)
	return f.String()
}

func (params *FbParams) SetParam(key string, value string) {
	switch key {
	case "host":
		params.Host = value
	case "port":
		params.Port = value
	case "database":
		params.Database = value
	case "user":
		params.User = value
	case "password":
		params.Password = value
	}
}

func (fb *FirebirdEngine) GetName() string {
	return fb.Name
}

func (fb *FirebirdEngine) CreateParams() interface{} {
	return &FbParams{}
}

func (fb *FirebirdEngine) LoadSchema(config *Config, reader ObjectReader) (ElementSchema, error) {
	schema := &FbSchema{
		BaseElementSchema{
			CoreElements: make(map[string]map[string]ElementSchema),
		},
	}

	var schemaElements []ElementSchema
	if config.AppType == AppTypeProject {
		if _, err := os.Stat("./" + config.AppName); os.IsNotExist(err) {
			return nil, errors.New(fmt.Sprintf("Folder for Project: %s doesn't exist", config.AppName))
		}

		elements, err := fb.LoadElementsFromFiles(config.AppName, schema, reader)
		if err != nil {
			return nil, err
		}
		schemaElements = append(schemaElements, elements...)

	} else {
		conn, err := fb.OpenConnection(config.Params.(*FbParams))
		if err != nil {
			return nil, err
		}
		elements, err := schema.FetchElementsFromDB(conn)
		if err != nil {
			return nil, err
		}
		schemaElements = append(schemaElements, elements...)

		fb.CloseConnection(conn)
	}

	for _, el := range schemaElements {
		err := schema.AddChild(el)
		if err != nil {
			return nil, err
		}
	}

	return schema, nil
}

type FbSchema struct {
	BaseElementSchema
}

func (fbs *FbSchema) GetChilds() []ElementSchema {
	var childs []ElementSchema
	for _, childsByType := range fbs.CoreElements {
		for _, child := range childsByType {
			childs = append(childs, child)
		}
	}
	return childs
}

func (fbs *FbSchema) GetGlobalChildElements() []ElementSchema {
	return []ElementSchema{&Domain{}, &Exception{}, &Generator{}, &Role{}, &Procedure{}, &View{}, &Table{}}
}

func (fb *FirebirdEngine) ExecuteSQL(config *Config, sqls []string) error {
	conn, err := fb.OpenConnection(config.Params.(*FbParams))
	if err != nil {
		return err
	}
	defer fb.CloseConnection(conn)
	for _, stmt := range sqls {
		_, err = conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fbs *FbSchema) AddChild(child ElementSchema) error {
	childType := child.GetTypeName()
	if ok := fbs.CoreElements[childType]; ok == nil {
		fbs.CoreElements[childType] = make(map[string]ElementSchema)
	}
	fbs.CoreElements[childType][child.GetName()] = child
	return nil
}

func (fbs *FbSchema) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	var elements []ElementSchema
	for _, el := range fbs.GetGlobalChildElements() {
		fetchedElements, err := el.FetchElementsFromDB(conn)
		if err != nil {
			return nil, err
		}
		elements = append(elements, fetchedElements...)
	}
	return elements, nil
}

func (fbs *FbSchema) String() string {
	return "schema"
}

func (fb *FirebirdEngine) OpenConnection(params *FbParams) (*sql.DB, error) {
	connectionString := fmt.Sprintf("%s:%s@%s:%s/%s",
		params.GetParam("User"),
		params.GetParam("Password"),
		params.GetParam("Host"),
		params.GetParam("Port"),
		params.GetParam("Database"))
	return sql.Open("firebirdsql", connectionString)
}

func (fb *FirebirdEngine) CloseConnection(conn *sql.DB) {
	conn.Close()
}

func (e *FirebirdEngine) SchemaDiff(source interface{}, target interface{}) []*DiffObject {
	var changes []*DiffObject
	sourceSchema := source.(*FbSchema)
	targetSchema := target.(*FbSchema)

	for _, el := range sourceSchema.GetGlobalChildElements() {
		changes = append(changes, e.CompareScheme(sourceSchema.CoreElements[el.GetTypeName()], targetSchema.CoreElements[el.GetTypeName()])...)
	}

	return changes
}
