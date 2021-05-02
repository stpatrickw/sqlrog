package main

import (
	"fmt"
	"github.com/fatih/color"
	. "github.com/stpatrickw/sqlrog/common"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	var (
		source string
		target string
		filter string
		apply  bool
	)
	diffCmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff command",
		Long:  "Comparison functionality",
		RunE: func(cmd *cobra.Command, args []string) error {
			SchemerConfig.Load(DefaultConfigFileName)
			if _, ok := SchemerConfig.Apps[source]; !ok {
				return errors.New("Source app is not found")
			}
			sourceApp := SchemerConfig.Apps[source]

			if _, ok := SchemerConfig.Apps[target]; !ok {
				target = SchemerConfig.Apps[source].Params.(Params).GetParam("Source")
				if _, ok = SchemerConfig.Apps[target]; !ok {
					return errors.New("Target app is not found")
				}
			}
			targetApp := SchemerConfig.Apps[target]

			if sourceApp.Engine != targetApp.Engine {
				return errors.New("Source and target app engines should be compatible.")
			}
			engine := Engines[sourceApp.Engine]
			type chanResult struct {
				Schema ElementSchema
				Error  error
			}
			sourceChan := make(chan chanResult)
			targetChan := make(chan chanResult)
			go func() {
				fmt.Println("Fetching source schema...")
				sourceSchema, err := engine.LoadSchema(sourceApp, &YamlSchemaReader{})
				sourceChan <- chanResult{
					Schema: sourceSchema,
					Error:  err,
				}
			}()
			go func() {
				fmt.Println("Fetching target schema...")
				targetSchema, err := engine.LoadSchema(targetApp, &YamlSchemaReader{})
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
				yellow.Println("There is nothing to change")
			} else {
				sort.Slice(diffs, func(i, j int) bool {
					return diffs[i].Priority > diffs[j].Priority
				})
				if apply {
					if err = engine.ApplyDiffs(targetApp, diffs, DEFAULT_SQL_SEPARATOR_WITH_RETURN); err != nil {
						return err
					}
				} else {
					fmt.Println("Diff SQL:")
					for _, change := range diffs {
						switch change.State {
						case DIFF_TYPE_DROP:
							red.Printf("%s\n", strings.Join(change.DiffSql(DEFAULT_SQL_SEPARATOR_WITH_RETURN), ""))
						case DIFF_TYPE_CREATE:
							green.Printf("%s\n", strings.Join(change.DiffSql(DEFAULT_SQL_SEPARATOR_WITH_RETURN), ""))
						case DIFF_TYPE_UPDATE:
							yellow.Printf("%s\n", strings.Join(change.DiffSql(DEFAULT_SQL_SEPARATOR_WITH_RETURN), ""))
						}
					}
				}
			}

			return nil
		},
	}

	diffCmd.Flags().StringVarP(&filter, "filter", "f", "", "Filter by element name")
	diffCmd.Flags().StringVarP(&source, "source", "s", "", "Source app")
	diffCmd.Flags().StringVarP(&target, "target", "t", "", "Target")
	diffCmd.Flags().BoolVarP(&apply, "apply", "a", false, "Apply changes for target")

	CliCommands = append(CliCommands, diffCmd)
}

func compareApps(engine Engine, sourceSchema ElementSchema, targetSchema ElementSchema) ([]*DiffObject, error) {
	fmt.Println("Comparing schemas...")
	changes := engine.SchemaDiff(sourceSchema, targetSchema)

	return changes, nil
}

func applyFilter(filter string, diffs []*DiffObject) []*DiffObject {
	if filter == "" {
		return diffs
	}
	filter = strings.ToUpper(filter)
	var newDiffs []*DiffObject
	for _, diff := range diffs {
		if (diff.To != nil && strings.Contains(strings.ToUpper(diff.To.GetName()), filter)) ||
			(diff.From != nil && strings.Contains(strings.ToUpper(diff.From.GetName()), filter)) {
			newDiffs = append(newDiffs, diff)
		}
	}

	return newDiffs
}
