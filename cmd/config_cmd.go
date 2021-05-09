package main

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
)

const SchemaFileTypeYml = "yml"

var validate *validator.Validate

func init() {
	validate = validator.New()
	config := &sqlrog.Config{}
	var (
		fileName   string
		sourceApp  string
		readerType string
	)

	showAppCmd := &cobra.Command{
		Use:           "show",
		Short:         "Show apps in config",
		Long:          "Show applications",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sqlrog.ProjectConfig.Load(fileName); err != nil {
				return err
			}

			for _, app := range sqlrog.ProjectConfig.Projects {
				sqlrog.Log("info", fmt.Sprintf("App: %s, Engine: %s, Type: %s\n", app.GetAppName(), app.GetEngineName(), app.AppType))
			}

			return nil
		},
	}

	addAppCmd := &cobra.Command{
		Use:           "add",
		Short:         "Add app to config",
		Long:          "Add application configuration",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if config.ProjectName == "" {
				return errors.New("App name should be set")
			}
			if err := sqlrog.ProjectConfig.Load(fileName); err != nil {
				return err
			}
			for _, appConfig := range sqlrog.ProjectConfig.Projects {
				if appConfig.ProjectName == config.ProjectName {
					return errors.New(fmt.Sprintf("Project with name '%s' already exists", config.ProjectName))
				}
			}
			if config.AppType == sqlrog.ProjectTypeFile {
				if sourceApp == "" {
					return errors.New("Source connection app should be set")
				}
				if _, ok := sqlrog.ProjectConfig.Projects[sourceApp]; !ok {
					return errors.New("Source connection app is not found")
				}
				if readerType == "" {
					readerType = SchemaFileTypeYml
				}

				var schemaWriter sqlrog.ObjectWriter
				switch readerType {
				case "yml":
					schemaWriter = &sqlrog.YamlSchemaWriter{}
				}

				config.Engine = sqlrog.ProjectConfig.Projects[sourceApp].Engine
				engine := sqlrog.Engines[config.Engine]
				config.Params = &sqlrog.ConfigParams{
					Source:   sourceApp,
					FileType: readerType,
				}
				sourceConfig := sqlrog.ProjectConfig.Projects[sourceApp]
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
					errs := err.(validator.ValidationErrors)
					for _, e := range errs {
						sqlrog.Logln("warn", "Argument '"+strings.ToLower(e.Field())+"' is missing.")
					}
					return errors.New("Arguments missing")
				}
				config.Params = configParams
			}

			return addAppToConfig(fileName, config)
		},
	}
	addAppCmd.Flags().StringVarP(&config.ProjectName, "name", "n", "", "Project name")
	addAppCmd.Flags().StringVarP(&config.Engine, "engine", "e", "", "Database adapter")
	addAppCmd.Flags().StringVarP(&config.AppType, "type", "t", "connection", "Project type (connection/project)")
	addAppCmd.Flags().StringVarP(&readerType, "readertype", "r", "yml", "Schema reader type (default is yml)")
	addAppCmd.Flags().StringVarP(&sourceApp, "source", "s", "", "Source connection App")
	addAppCmd.Flags().StringVarP(&fileName, "config", "c", sqlrog.DefaultConfigFileName, "Config file name")
	showAppCmd.Flags().StringVarP(&fileName, "config", "c", sqlrog.DefaultConfigFileName, "Config file name")

	CliCommands = append(CliCommands, addAppCmd, showAppCmd)
}

func addAppToConfig(fileName string, config *sqlrog.Config) error {
	for _, appConfig := range sqlrog.ProjectConfig.Projects {
		if appConfig.ProjectName == config.ProjectName {
			return errors.New(fmt.Sprintf("Project with name '%s' was overrided", config.ProjectName))
		}
	}
	sqlrog.ProjectConfig.Projects[config.GetAppName()] = config

	if err := sqlrog.ProjectConfig.Save(fileName); err != nil {
		return err
	}
	sqlrog.Log("info", fmt.Sprintf("New app %s was added to config", config.GetAppName()))

	return nil
}
