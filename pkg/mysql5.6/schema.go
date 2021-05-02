package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	. "github.com/stpatrickw/sqlrog/common"
	"os"
	"reflect"
	"sort"
)

type MysqlEngine struct {
	CoreEngine
}

func init() {
	mysql := &MysqlEngine{
		CoreEngine{
			Name:  "MySql",
			Alias: "mysql5.6",
		},
	}
	Engines[mysql.Alias] = mysql
}

type MysqlParams struct {
	Host     string `yaml:"host" validate:"required"`
	Port     string `yaml:"port" validate:"required"`
	Database string `yaml:"database" validate:"required"`
	User     string `yaml:"user" validate:"required"`
	Password string `yaml:"password" validate:"required"`
}

func (params *MysqlParams) GetParam(key string) string {
	r := reflect.ValueOf(params)
	f := reflect.Indirect(r).FieldByName(key)
	return f.String()
}

func (params *MysqlParams) SetParam(key string, value string) {
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

func (my *MysqlEngine) GetName() string {
	return my.Name
}

func (my *MysqlEngine) CreateParams() interface{} {
	return &MysqlParams{}
}

func (my *MysqlEngine) LoadSchema(config *Config, reader ObjectReader) (ElementSchema, error) {
	schema := &MysqlSchema{
		BaseElementSchema{
			CoreElements: make(map[string]map[string]ElementSchema),
		},
	}

	var schemaElements []ElementSchema
	if config.AppType == AppTypeProject {
		if _, err := os.Stat("./" + config.AppName); os.IsNotExist(err) {
			return nil, errors.New(fmt.Sprintf("Folder for Project: %s doesn't exist", config.AppName))
		}

		elements, err := my.LoadElementsFromFiles(config.AppName, schema, reader)
		if err != nil {
			return nil, err
		}
		schemaElements = append(schemaElements, elements...)

	} else {
		conn, err := my.OpenConnection(config.Params.(*MysqlParams))
		if err != nil {
			return nil, err
		}
		elements, err := schema.FetchElementsFromDB(conn)
		if err != nil {
			return nil, err
		}
		schemaElements = append(schemaElements, elements...)

		my.CloseConnection(conn)
	}

	for _, el := range schemaElements {
		err := schema.AddChild(el)
		if err != nil {
			return nil, err
		}
	}

	return schema, nil
}

func (my *MysqlEngine) ExecuteSQL(config *Config, sqls []string) error {
	conn, err := my.OpenConnection(config.Params.(*MysqlParams))
	if err != nil {
		return err
	}
	defer my.CloseConnection(conn)
	for _, stmt := range sqls {
		_, err = conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (my *MysqlEngine) OpenConnection(params *MysqlParams) (*sql.DB, error) {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		params.GetParam("User"),
		params.GetParam("Password"),
		params.GetParam("Host"),
		params.GetParam("Port"),
		params.GetParam("Database"))
	return sql.Open("mysql", connectionString)
}

func (fb *MysqlEngine) CloseConnection(conn *sql.DB) {
	conn.Close()
}

func (my *MysqlEngine) SchemaDiff(source interface{}, target interface{}) []*DiffObject {
	var changes []*DiffObject
	sourceSchema := source.(*MysqlSchema)
	targetSchema := target.(*MysqlSchema)

	for _, el := range sourceSchema.GetGlobalChildElements() {
		changes = append(changes, my.CompareScheme(sourceSchema.CoreElements[el.GetTypeName()], targetSchema.CoreElements[el.GetTypeName()])...)
	}
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Priority > changes[j].Priority
	})

	return changes
}

type MysqlSchema struct {
	BaseElementSchema
}

func (mys *MysqlSchema) GetChilds() []ElementSchema {
	var childs []ElementSchema
	for _, childsByType := range mys.CoreElements {
		for _, child := range childsByType {
			childs = append(childs, child)
		}
	}
	return childs
}

func (mys *MysqlSchema) GetGlobalChildElements() []ElementSchema {
	return []ElementSchema{&Table{}, &View{}, &Function{}, &Procedure{}}
}

func (mys *MysqlSchema) AddChild(child ElementSchema) error {
	childType := child.GetTypeName()
	if ok := mys.CoreElements[childType]; ok == nil {
		mys.CoreElements[childType] = make(map[string]ElementSchema)
	}
	mys.CoreElements[childType][child.GetName()] = child
	return nil
}

func (mys *MysqlSchema) FetchElementsFromDB(conn *sql.DB) ([]ElementSchema, error) {
	var elements []ElementSchema
	for _, el := range mys.GetGlobalChildElements() {
		fetchedElements, err := el.FetchElementsFromDB(conn)
		if err != nil {
			return nil, err
		}
		elements = append(elements, fetchedElements...)
	}
	return elements, nil
}

func (mys *MysqlSchema) String() string {
	return "schema"
}
