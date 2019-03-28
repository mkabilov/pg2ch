package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
)

var (
	configFile    = flag.String("config", "config.yaml", "path to the config file")
	generateChDDL = flag.Bool("generate-ch-ddl", false, "generates clickhouse's tables ddl")
	Version       = "devel"
	Revision      = "devel"

	GoVersion = runtime.Version()
)

func buildInfo() string {
	return fmt.Sprintf("Postgresql to Clickhouse replicator %s git revision %s go version %s", Version, Revision, GoVersion)
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", buildInfo())
		fmt.Fprintf(os.Stderr, "\nUsage:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *configFile == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	cfg, err := config.New(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not load config: %v\n", err)
		os.Exit(1)
	}

	repl := replicator.New(*cfg)
	if *generateChDDL {
		if err := repl.GenerateChDDL(); err != nil {
			fmt.Fprintf(os.Stderr, "could not create tables on the clickhouse side: %v\n", err)
			os.Exit(1)
		}
	} else {
		if err := repl.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "could not start: %v\n", err)
			os.Exit(1)
		}
	}
}
