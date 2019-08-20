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
	configFile      = flag.String("config", "config.yaml", "path to the config file")
	generateChDDL   = flag.Bool("generate-ch-ddl", false, "generates clickhouse's tables ddl")
	printTablesLSN  = flag.Bool("print-lsn", false, "print saved LSNs for replicated tables")
	onlySync        = flag.Bool("sync", false, "sync tables and exit")
	preparePgTables = flag.Bool("prepare-pg-tables", false, "Set replica identity for the tables, add tables to the publication")
	Version         = "devel"
	Revision        = "devel"

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

	repl := replicator.New(cfg)

	switch {
	case *generateChDDL:
		if err := repl.GenerateChDDL(); err != nil {
			fmt.Fprintf(os.Stderr, "could not create tables on the clickhouse side: %v\n", err)
			os.Exit(1)
		}
	case *printTablesLSN:
		if err := repl.Init(); err != nil {
			fmt.Fprintf(os.Stderr, "could not init tables: %v\n", err)
			os.Exit(1)
		}
		repl.PrintTablesLSN()
	case *onlySync:
		var err error

		if err = repl.Init(); err == nil {
			var tablesToSync []config.PgTableName

			if tablesToSync, err = repl.GetTablesToSync(); err == nil {
				err = repl.SyncTables(tablesToSync, false)
			}
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "could not sync tables: %v\n", err)
			os.Exit(1)
		}
	case *preparePgTables:
		if err := repl.PreparePgTables(); err != nil {
			fmt.Fprintf(os.Stderr, "could not set replica identity: %v\n", err)
			os.Exit(1)
		}
	default:
		cfg.Print()
		if err := repl.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "could not start: %v\n", err)
			os.Exit(1)
		}

	}
}
