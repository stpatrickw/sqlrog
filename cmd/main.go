package main

import (
	"github.com/spf13/cobra"
	_ "github.com/stpatrickw/sqlrog/internal/firebird2.5"
	_ "github.com/stpatrickw/sqlrog/internal/mysql5.6"
	"github.com/stpatrickw/sqlrog/internal/sqlrog"
	"log"
	"os"
)

var CliCommands []*cobra.Command

func main() {
	if _, err := os.Stat(sqlrog.DefaultConfigFileName); os.IsNotExist(err) {
		if _, err = os.Create(sqlrog.DefaultConfigFileName); err != nil {
			log.Fatal(err)
		}
	}
	if err := sqlrog.AppConfig.Load(sqlrog.DefaultConfigFileName); err != nil {
		log.Fatal(err)
	}

	rootCmd := &cobra.Command{}
	for _, command := range CliCommands {
		rootCmd.AddCommand(command)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
