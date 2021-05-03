package main

import (
	"fmt"
	"github.com/go-playground/validator"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"log"
	"strings"
)

const SchemaFileTypeYml = "yml"

var validate *validator.Validate

func init() {
	validate = validator.New()
	config := &sqlrog.Config{}
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
			sqlrog.AppConfig.Load(fileName)

			for _, app := range sqlrog.AppConfig.Apps {
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
			if config.AppType == sqlrog.AppTypeProject {
				if sourceApp == "" {
					return errors.New("Source connection app should be set")
				}
				sqlrog.AppConfig.Load(fileName)
				if _, ok := sqlrog.AppConfig.Apps[sourceApp]; !ok {
					return errors.New("Source connection app is not found")
				}
				if fileType == "" {
					fileType = SchemaFileTypeYml
				}

				var schemaWriter sqlrog.ObjectWriter
				switch fileType {
				case "yml":
					schemaWriter = &sqlrog.YamlSchemaWriter{}
				}

				config.Engine = sqlrog.AppConfig.Apps[sourceApp].Engine
				engine := sqlrog.Engines[config.Engine]
				config.Params = &sqlrog.ConfigParams{
					Source:   sourceApp,
					FileType: fileType,
				}
				sourceConfig := sqlrog.AppConfig.Apps[sourceApp]
				schema, err := engine.LoadSchema(sourceConfig, &sqlrog.YamlSchemaReader{})
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
				if _, ok := sqlrog.Engines[config.Engine]; !ok {
					return errors.New("Unrecognized Engine")
				}
				configParams := sqlrog.Engines[config.Engine].CreateParams().(sqlrog.Params)
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
	addAppCmd.Flags().StringVarP(&fileName, "filename", "f", sqlrog.DefaultConfigFileName, "Config file name")
	showAppCmd.Flags().StringVarP(&fileName, "filename", "f", sqlrog.DefaultConfigFileName, "Config file name")

	CliCommands = append(CliCommands, addAppCmd, showAppCmd)
}

func addAppToConfig(fileName string, config *sqlrog.Config, isDefault bool) error {
	sqlrog.AppConfig.Load(fileName)

	for _, appConfig := range sqlrog.AppConfig.Apps {
		if appConfig.AppName == config.AppName {
			return errors.New(fmt.Sprintf("App with name '%s' already exists", config.AppName))
		}
	}
	sqlrog.AppConfig.Apps[config.GetAppName()] = config
	if isDefault || len(sqlrog.AppConfig.Apps) == 1 {
		sqlrog.AppConfig.DefaultApp = config.AppName
	}

	if err := sqlrog.AppConfig.Save(fileName); err != nil {
		return err
	}
	log.Printf("New app %s was added to config", config.GetAppName())

	return nil
}
