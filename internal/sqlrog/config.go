package sqlrog

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"gopkg.in/yaml.v2"
)

const DefaultConfigFileName = "config.yml"
const ProjectTypeFile = "file"

var Engines map[string]Engine
var ProjectConfig *ProjectsConfig

func init() {
	Engines = make(map[string]Engine)
	ProjectConfig = &ProjectsConfig{
		Projects: make(map[string]*Config),
	}
}

type ProjectsConfig struct {
	Projects map[string]*Config
}

type Params interface {
	GetParam(string) string
	SetParam(string, string)
}

type ConfigParams struct {
	Source   string `yaml:"source"`
	FileType string `yaml:"filetype"`
}

func (cp *ConfigParams) GetParam(key string) string {
	r := reflect.ValueOf(cp)
	f := reflect.Indirect(r).FieldByName(key)
	return f.String()
}

func (cp *ConfigParams) SetParam(key string, value string) {
	switch key {
	case "source":
		cp.Source = value
	case "filetype":
		cp.FileType = value
	}
}

type Configurable interface {
	GetEngineName() string
	GetAppName() string
	Load() error
}

type Config struct {
	ProjectName string      `yaml:"project_name" validate:"required"`
	Engine      string      `yaml:"engine" validate:"required"`
	AppType     string      `yaml:"type" validate:"required"`
	Params      interface{} `yaml:"params" validate:"required"`
}

func (conf *Config) GetEngineName() string {
	return conf.Engine
}
func (conf *Config) GetAppName() string {
	return conf.ProjectName
}

func (sc *ProjectsConfig) Load(fileName string) error {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		if _, err = os.Create(fileName); err != nil {
			return err
		}
	}
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &ProjectConfig)
	if err != nil {
		return err
	}
	for _, app := range ProjectConfig.Projects {
		var params Params
		if app.AppType == "project" {
			params = &ConfigParams{}
		} else {
			if _, ok := Engines[app.Engine]; !ok {
				return errors.New(fmt.Sprintf("Engine %s is not found", app.Engine))
			}
			params = Engines[app.Engine].CreateParams().(Params)
		}
		for key, val := range app.Params.(map[interface{}]interface{}) {
			params.SetParam(key.(string), val.(string))
		}

		app.Params = params
	}

	return nil
}

func (sc *ProjectsConfig) Save(fileName string) error {
	d, err := yaml.Marshal(&ProjectConfig)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fileName, d, 0644)
	if err != nil {
		return err
	}

	return nil
}

type YamlConfig struct {
	Config
	YamlFile string
}

func (yml *YamlConfig) Load() error {
	data, err := ioutil.ReadFile(yml.YamlFile)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &yml.Config)
	if err != nil {
		return err
	}

	return nil
}

type YamlSchemaWriter struct {
}

func (yml *YamlSchemaWriter) Write(object interface{}, fileName string) error {
	fileName = filepath.FromSlash(fileName)
	d, err := yaml.Marshal(object)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fileName, d, 0655)
}

type YamlSchemaReader struct {
}

func (yml *YamlSchemaReader) Read(fileName string) ([]byte, error) {
	fileName = filepath.FromSlash(fileName)
	return ioutil.ReadFile(fileName)
}
