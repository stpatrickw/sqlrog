package main

import (
	"sort"
	"strings"

	"github.com/fatih/color"

	"github.com/stpatrickw/sqlrog/internal/sqlrog"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	var (
		fileName string
		source   string
		target   string
		filter   string
		apply    bool
	)
	diffCmd := &cobra.Command{
		Use:           "diff",
		Short:         "Diff command",
		Long:          "Comparison functionality",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sqlrog.ProjectConfig.Load(fileName); err != nil {
				return err
			}
			if _, ok := sqlrog.ProjectConfig.Projects[source]; !ok {
				return errors.New("Source app is not found")
			}
			sourceApp := sqlrog.ProjectConfig.Projects[source]

			if _, ok := sqlrog.ProjectConfig.Projects[target]; !ok {
				target = sqlrog.ProjectConfig.Projects[source].Params.(sqlrog.Params).GetParam("Source")
				if _, ok = sqlrog.ProjectConfig.Projects[target]; !ok {
					return errors.New("Target app is not found")
				}
			}
			targetApp := sqlrog.ProjectConfig.Projects[target]

			if sourceApp.Engine != targetApp.Engine {
				return errors.New("Source and target app engines should be compatible.")
			}
			engine := sqlrog.Engines[sourceApp.Engine]
			type chanResult struct {
				Schema sqlrog.ElementSchema
				Error  error
			}
			sourceChan := make(chan chanResult)
			targetChan := make(chan chanResult)
			go func() {
				sqlrog.Logln("info", "Fetching source schema...")
				sourceSchema, err := engine.LoadSchema(sourceApp, &sqlrog.YamlSchemaReader{})
				sourceChan <- chanResult{
					Schema: sourceSchema,
					Error:  err,
				}
			}()
			go func() {
				sqlrog.Logln("info", "Fetching target schema...")
				targetSchema, err := engine.LoadSchema(targetApp, &sqlrog.YamlSchemaReader{})
				targetChan <- chanResult{
					Schema: targetSchema,
					Error:  err,
				}
			}()
			sourceResult := <-sourceChan
			targetResult := <-targetChan
			if sourceResult.Error != nil {
				return sourceResult.Error
			}
			if targetResult.Error != nil {
				return targetResult.Error
			}
			sourceSchema := sourceResult.Schema
			targetSchema := targetResult.Schema

			diffs, err := compareApps(engine, sourceSchema, targetSchema)
			if err != nil {
				return err
			}
			red := color.New(color.FgRed)
			green := color.New(color.FgHiGreen)
			yellow := color.New(color.FgYellow)

			diffs = applyFilter(filter, diffs)

			if len(diffs) == 0 {
				sqlrog.Logln("warn", "There is nothing to change")
			} else {
				sort.Slice(diffs, func(i, j int) bool {
					return diffs[i].Priority > diffs[j].Priority
				})
				if apply {
					if err = engine.ApplyDiffs(targetApp, diffs, sqlrog.DEFAULT_SQL_SEP_WITH_RETURN); err != nil {
						return err
					}
				} else {
					sqlrog.Logln("info", "Diff SQL:")
					for _, change := range diffs {
						switch change.State {
						case sqlrog.DIFF_TYPE_DROP:
							red.Printf("%s\n", strings.Join(change.DiffSql(sqlrog.DEFAULT_SQL_SEP), ""))
						case sqlrog.DIFF_TYPE_CREATE:
							green.Printf("%s\n", strings.Join(change.DiffSql(sqlrog.DEFAULT_SQL_SEP), ""))
						case sqlrog.DIFF_TYPE_UPDATE:
							yellow.Printf("%s\n", strings.Join(change.DiffSql(sqlrog.DEFAULT_SQL_SEP), ""))
						}
					}
				}
			}

			return nil
		},
	}

	diffCmd.Flags().StringVarP(&filter, "filter", "f", "", "Filter by element name")
	diffCmd.Flags().StringVarP(&source, "source", "s", "", "Source project")
	diffCmd.Flags().StringVarP(&target, "target", "t", "", "Target project")
	diffCmd.Flags().BoolVarP(&apply, "apply", "a", false, "Apply changes for target")
	diffCmd.Flags().StringVarP(&fileName, "config", "c", sqlrog.DefaultConfigFileName, "Config file name")

	CliCommands = append(CliCommands, diffCmd)
}

func compareApps(engine sqlrog.Engine, sourceSchema sqlrog.ElementSchema, targetSchema sqlrog.ElementSchema) ([]*sqlrog.DiffObject, error) {
	sqlrog.Logln("info", "Comparing schemas...")
	changes := engine.SchemaDiff(sourceSchema, targetSchema)

	return changes, nil
}

func applyFilter(filter string, diffs []*sqlrog.DiffObject) []*sqlrog.DiffObject {
	if filter == "" {
		return diffs
	}
	filter = strings.ToUpper(filter)
	var newDiffs []*sqlrog.DiffObject
	for _, diff := range diffs {
		if (diff.To != nil && strings.Contains(strings.ToUpper(diff.To.GetName()), filter)) ||
			(diff.From != nil && strings.Contains(strings.ToUpper(diff.From.GetName()), filter)) {
			newDiffs = append(newDiffs, diff)
		}
	}

	return newDiffs
}
