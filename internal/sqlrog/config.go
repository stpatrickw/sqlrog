package sqlrog

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"reflect"
)

const DefaultConfigFileName = "config.yml"
const AppTypeProject = "project"

var Engines map[string]Engine
var AppConfig *AppsConfig

func init() {
	Engines = make(map[string]Engine)
	AppConfig = &AppsConfig{
		Apps: make(map[string]*Config),
	}
}

type AppsConfig struct {
	Apps       map[string]*Config
	DefaultApp string `yaml:"default_app"`
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
	AppName string      `yaml:"app_name" validate:"required"`
	Engine  string      `yaml:"engine" validate:"required"`
	AppType string      `yaml:"type" validate:"required"`
	Params  interface{} `yaml:"params" validate:"required"`
}

func (conf *Config) GetEngineName() string {
	return conf.Engine
}
func (conf *Config) GetAppName() string {
	return conf.AppName
}

func (sc *AppsConfig) Load(fileName string) error {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &AppConfig)
	if err != nil {
		return err
	}
	for _, app := range AppConfig.Apps {
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

func (sc *AppsConfig) Save(fileName string) error {
	d, err := yaml.Marshal(&AppConfig)
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
