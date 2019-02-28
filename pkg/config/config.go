package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"gopkg.in/yaml.v2"
)

const (
	CollapsingMergeTree TableEngine = iota
	ReplacingMergeTree
	VersionedCollapsingMergeTree
	MergeTree
)

var tableEngines = map[TableEngine]string{
	CollapsingMergeTree:          "CollapsingMergeTree",
	ReplacingMergeTree:           "ReplacingMergeTree",
	VersionedCollapsingMergeTree: "VersionedCollapsingMergeTree",
	MergeTree:                    "MergeTree",
}

type TableEngine int

type DbConfig struct {
	pgx.ConnConfig      `yaml:",inline"`
	ReplicationSlotName string `yaml:"replicationSlotName"`
	PublicationName     string `yaml:"publicationName"`
}

type Column struct {
	ChName     string  `yaml:"name"`
	ChType     string  `yaml:"type"`
	EmptyValue *string `yaml:"empty_value"`
	Nullable   bool    `yaml:"nullable"`
}

type ColumnMapping []map[string]Column

type Table struct {
	Columns                 ColumnMapping `yaml:"columns"`
	SignColumn              string        `yaml:"sign_column"`
	BufferRowIdColumn       string        `yaml:"buffer_row_id"`
	BufferTable             string        `yaml:"buffer_table"`
	BufferSize              int           `yaml:"buffer_size"`
	MainTable               string        `yaml:"main_table"`
	VerColumn               string        `yaml:"ver_column"`
	Engine                  TableEngine   `yaml:"engine"`
	MergeThreshold          int           `yaml:"merge_treshold"`
	InactivityMergeTimeout  time.Duration `yaml:"inactivity_merge_timeout"`
	SkipInitSync            bool          `yaml:"skip_init_sync"`
	InitSyncSkipBufferTable bool          `yaml:"init_sync_skip_buffer_table"`
}

type Config struct {
	CHConnectionString string           `yaml:"clickhouse"`
	Pg                 DbConfig         `yaml:"pg"`
	Tables             map[string]Table `yaml:"tables"`
}

func (t TableEngine) String() string {
	return tableEngines[t]
}

func (t TableEngine) MarshalYAML() (interface{}, error) {
	return tableEngines[t], nil
}

func (t *TableEngine) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	for k, v := range tableEngines {
		if strings.ToLower(val) == strings.ToLower(v) {
			*t = k
			return nil
		}
	}

	return fmt.Errorf("unknown table engine: %q", val)
}

func New(filepath string) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode yaml: %v", err)
	}

	if cfg.Pg.PublicationName == "" {
		return nil, fmt.Errorf("publication name is not specified")
	}

	if cfg.Pg.ReplicationSlotName == "" {
		return nil, fmt.Errorf("replication slot name is not specified")
	}

	return &cfg, nil
}
