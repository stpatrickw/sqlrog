package main

import (
	"github.com/spf13/cobra"
	_ "github.com/stpatrickw/sqlrog/internal/firebird2.5"
	_ "github.com/stpatrickw/sqlrog/internal/mysql5.6"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
)

var CliCommands []*cobra.Command

func main() {
	rootCmd := &cobra.Command{}
	for _, command := range CliCommands {
		rootCmd.AddCommand(command)
	}

	if err := rootCmd.Execute(); err != nil {
		sqlrog.Log("error", err.Error())
	}
}
