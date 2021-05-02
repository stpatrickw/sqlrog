package main

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	. "github.com/stpatrickw/sqlrog/common"
	"strings"
)

const SchemaFileTypeYml = "yml"

var validate *validator.Validate

func init() {
	validate = validator.New()
	config := &Config{}
	var isDefault bool
	var (
		fileName  string
		sourceApp string
		fileType  string
	)

	showAppCmd := &cobra.Command{
		Use:   "show",
		Short: "Show apps in config",
		Long:  "Show applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			SchemerConfig.Load(fileName)

			for _, app := range SchemerConfig.Apps {
				fmt.Printf("App: %s, Engine: %s, Type: %s\n", app.GetAppName(), app.GetEngineName(), app.AppType)
			}

			return nil
		},
	}

	addAppCmd := &cobra.Command{
		Use:   "add",
		Short: "Add app to config",
		Long:  "Add application configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			if config.AppName == "" {
				return errors.New("App name should be set")
			}
			if config.AppType == AppTypeProject {
				if sourceApp == "" {
					return errors.New("Source connection app should be set")
				}
				SchemerConfig.Load(fileName)
				if _, ok := SchemerConfig.Apps[sourceApp]; !ok {
					return errors.New("Source connection app is not found")
				}
				if fileType == "" {
					fileType = SchemaFileTypeYml
				}

				var schemaWriter ObjectWriter
				switch fileType {
				case "yml":
					schemaWriter = &YamlSchemaWriter{}
				}

				config.Engine = SchemerConfig.Apps[sourceApp].Engine
				engine := Engines[config.Engine]
				config.Params = &ConfigParams{
					Source:   sourceApp,
					FileType: fileType,
				}
				sourceConfig := SchemerConfig.Apps[sourceApp]
				schema, err := engine.LoadSchema(sourceConfig, &YamlSchemaReader{})
				if err != nil {
					return err
				}
				err = engine.SaveSchemaToFiles(config, schema, schemaWriter)
				if err != nil {
					return err
				}
			} else {
				if config.Engine == "" {
					return errors.New("Engine should be set")
				}
				if _, ok := Engines[config.Engine]; !ok {
					return errors.New("Unrecognized Engine")
				}
				configParams := Engines[config.Engine].CreateParams().(Params)
				for _, arg := range args {
					param := strings.SplitN(arg, "=", 2)
					if len(param) != 2 {
						return errors.New(fmt.Sprintf("Parameters %v unexpected error", param))
					}
					configParams.SetParam(param[0], param[1])
				}
				err := validate.Struct(configParams)
				if err != nil {
					return err
				}
				config.Params = configParams
			}

			return addAppToConfig(fileName, config, isDefault)
		},
	}
	addAppCmd.Flags().StringVarP(&config.AppName, "name", "n", "", "App name")
	addAppCmd.Flags().StringVarP(&config.Engine, "engine", "e", "", "Database driver")
	addAppCmd.Flags().StringVarP(&config.AppType, "type", "t", "connection", "App type (connection/project)")
	addAppCmd.Flags().StringVarP(&fileType, "filetype", "", "yml", "Project file type")
	addAppCmd.Flags().StringVarP(&sourceApp, "source", "s", "", "Source connection App")
	addAppCmd.Flags().BoolVarP(&isDefault, "default", "d", false, "Is database default")
	addAppCmd.Flags().StringVarP(&fileName, "filename", "f", DefaultConfigFileName, "Config file name")
	showAppCmd.Flags().StringVarP(&fileName, "filename", "f", DefaultConfigFileName, "Config file name")

	CliCommands = append(CliCommands, addAppCmd, showAppCmd)
}

func addAppToConfig(fileName string, config *Config, isDefault bool) error {
	SchemerConfig.Load(fileName)

	for _, appConfig := range SchemerConfig.Apps {
		if appConfig.AppName == config.AppName {
			return errors.New(fmt.Sprintf("App with name '%s' already exists", config.AppName))
		}
	}
	SchemerConfig.Apps[config.GetAppName()] = config
	if isDefault || len(SchemerConfig.Apps) == 1 {
		SchemerConfig.DefaultApp = config.AppName
	}

	return SchemerConfig.Save(fileName)
}
