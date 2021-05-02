package main

import (
	"github.com/spf13/cobra"
	. "github.com/stpatrickw/sqlrog/common"
	_ "github.com/stpatrickw/sqlrog/pkg/firebird2.5"
	_ "github.com/stpatrickw/sqlrog/pkg/mysql5.6"
	"log"
	"time"
)

var CliCommands []*cobra.Command

func main() {
	start := time.Now()
	if err := SchemerConfig.Load(DefaultConfigFileName); err != nil {
		log.Fatal(err)
	}

	rootCmd := &cobra.Command{}
	for _, command := range CliCommands {
		rootCmd.AddCommand(command)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
	log.Printf("Execution time: %d sec", int32(time.Since(start).Seconds()))
}
