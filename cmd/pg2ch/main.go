package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/ikitiki/pg2ch/pkg/config"
	"github.com/ikitiki/pg2ch/pkg/replicator"
)

var (
	configFile = flag.String("config", "config.yaml", "path to the config file")
	Version    = "devel"
	Revision   = "devel"

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
		fmt.Fprintf(os.Stderr, "could not load config: %v", err)
		os.Exit(1)
	}

	if err := replicator.New(*cfg).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "could not start: %v", err)
		os.Exit(1)
	}
}
