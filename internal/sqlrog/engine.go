package sqlrog

import (
	"fmt"
	"github.com/fatih/color"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
)

const (
	DIFF_TYPE_DROP = iota + 1
	DIFF_TYPE_UPDATE
	DIFF_TYPE_CREATE
	DEFAULT_SQL_SEPARATOR             = ";"
	DEFAULT_SQL_SEPARATOR_WITH_RETURN = ";\n"
)

type Engine interface {
	GetName() string
	CreateParams() interface{}
	LoadSchema(config *Config, reader ObjectReader) (ElementSchema, error)
	SaveSchemaToFiles(config *Config, schema ElementSchema, writer ObjectWriter) error
	SaveElementSchemaToFile(config *Config, schema ElementSchema, writer ObjectWriter) error
	DeleteElementSchemaFile(config *Config, schema ElementSchema) error
	ExecuteSQL(config *Config, sqls []string) error
	ApplyDiffs(config *Config, diffs []*DiffObject, sep string) error
	SchemaDiff(src interface{}, dest interface{}) []*DiffObject
}

type CoreEngine struct {
	Name         string
	Alias        string
	Config       Configurable
	SchemaWriter ObjectWriter
	SchemaReader ObjectReader
}

type ObjectWriter interface {
	Write(interface{}, string) error
}
type ObjectReader interface {
	Read(string) ([]byte, error)
}

func (e *CoreEngine) LoadElementsFromFiles(appName string, schema ElementSchema, reader ObjectReader) ([]ElementSchema, error) {
	var elements []ElementSchema
	for _, el := range schema.GetGlobalChildElements() {
		elType := reflect.TypeOf(el).Elem()
		files, err := ioutil.ReadDir("./" + appName + "/" + el.GetPluralTypeName())
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			newElement := reflect.New(elType)
			element := newElement.Interface().(ElementSchema)
			data, err := reader.Read("./" + appName + "/" + el.GetPluralTypeName() + "/" + f.Name())
			if err != nil {
				return nil, err
			}
			err = yaml.Unmarshal(data, element)
			if err != nil {
				return nil, err
			}
			elements = append(elements, element)
		}
	}
	return elements, nil
}

func (c *CoreEngine) SaveSchemaToFiles(config *Config, schema ElementSchema, writer ObjectWriter) error {
	for _, element := range schema.GetChilds() {
		err := c.SaveElementSchemaToFile(config, element, writer)
		if err != nil {
			return err
		}
	}

	log.Printf("Schema saved successfully \n")

	return nil
}

func (e *CoreEngine) CompareScheme(source interface{}, target interface{}) []*DiffObject {
	var changes []*DiffObject
	sourceSchema := e.CastInterfaceToMapElementSchema(source)
	targetSchema := e.CastInterfaceToMapElementSchema(target)

	for key, value := range sourceSchema {
		if _, ok := targetSchema[key]; !ok {
			changes = append(changes, value.DiffsOnCreate(value)...)
		} else {
			diff := value.Diff(targetSchema[key])
			if diff != nil {
				changes = append(changes, diff)
			}
		}
	}
	for key, value := range targetSchema {
		if _, ok := sourceSchema[key]; !ok {
			changes = append(changes, value.DiffsOnDrop(value)...)
		}
	}

	return changes
}

func (e *CoreEngine) ApplyDiffs(config *Config, diffs []*DiffObject, sep string) error {
	fmt.Println("Applying updates...")
	if config.AppType == AppTypeProject {
		for _, diff := range diffs {
			switch diff.State {
			case DIFF_TYPE_CREATE, DIFF_TYPE_UPDATE:
				err := Engines[config.Engine].SaveElementSchemaToFile(config, diff.To, &YamlSchemaWriter{})
				if err != nil {
					return err
				}
			case DIFF_TYPE_DROP:
				err := Engines[config.Engine].DeleteElementSchemaFile(config, diff.From)
				if err != nil {
					return err
				}
			}
		}
	} else {
		green := color.New(color.FgHiGreen)
		cyan := color.New(color.FgCyan)
		yellow := color.New(color.FgYellow)
		for _, diff := range diffs {
			for _, stmt := range diff.DiffSql(sep) {
				cyan.Print("Applying: ... \n")
				yellow.Print(stmt)
				if err := Engines[config.Engine].ExecuteSQL(config, []string{stmt}); err != nil {
					return err
				}
				green.Printf(" Done\n")
			}
		}
	}

	return nil
}

func (c *CoreEngine) SaveElementSchemaToFile(config *Config, element ElementSchema, writer ObjectWriter) error {
	appName := config.GetAppName()
	path := "./" + appName + "/" + element.GetPluralTypeName()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
	}

	err := writer.Write(element, path+"/"+strings.Trim(element.GetName(), " ")+".yaml")
	if err != nil {
		return err
	}

	return nil
}

func (c *CoreEngine) DeleteElementSchemaFile(config *Config, element ElementSchema) error {
	appName := config.GetAppName()
	path := "./" + appName + "/" + element.GetPluralTypeName() + "/" + strings.Trim(element.GetName(), " ") + ".yaml"
	if _, err := os.Stat(path); err == nil {
		return os.Remove(path)
	}

	return nil
}

func (e *CoreEngine) CastInterfaceToMapElementSchema(source interface{}) map[string]ElementSchema {
	src := make(map[string]ElementSchema)
	v := reflect.ValueOf(source)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			src[key.Interface().(string)] = v.MapIndex(key).Interface().(ElementSchema)
		}
	}
	return src
}

func (e *CoreEngine) Equals(source interface{}, destination interface{}) bool {
	src := e.CastInterfaceToMapElementSchema(source)
	dest := e.CastInterfaceToMapElementSchema(destination)
	if len(src) != len(dest) {
		return false
	}
	for name, element := range src {
		if _, ok := dest[name]; !ok {
			return false
		}
		if !(element).Equals(dest[name]) {
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

type DiffObject struct {
	State    int
	Type     string
	From     ElementSchema
	To       ElementSchema
	Priority int
}

func (o *DiffObject) DiffSql(sep string) []string {
	switch o.State {
	case DIFF_TYPE_CREATE:
		return o.To.CreateDefinition(sep)
	case DIFF_TYPE_DROP:
		return o.From.DropDefinition(sep)
	case DIFF_TYPE_UPDATE:
		return o.From.AlterDefinition(o.To, sep)
	}
	return []string{}
}
