package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"gopkg.in/yaml.v2"
)

const defaultInactivityMergeTimeout = time.Minute

type tableEngine int

const (
	// CollapsingMergeTree represents CollapsingMergeTree table engine
	CollapsingMergeTree tableEngine = iota

	//ReplacingMergeTree represents ReplacingMergeTree table engine
	ReplacingMergeTree

	//VersionedCollapsingMergeTree represents VersionedCollapsingMergeTree table engine
	VersionedCollapsingMergeTree

	//MergeTree represents MergeTree table engine
	MergeTree
)

var tableEngines = map[tableEngine]string{
	CollapsingMergeTree:          "CollapsingMergeTree",
	ReplacingMergeTree:           "ReplacingMergeTree",
	VersionedCollapsingMergeTree: "VersionedCollapsingMergeTree",
	MergeTree:                    "MergeTree",
}

type dbConfig struct {
	pgx.ConnConfig `yaml:",inline"`

	ReplicationSlotName string `yaml:"replication_slot_name"`
	PublicationName     string `yaml:"publication_name"`
}

// Column contains information about the table column
type Column struct {
	ChName     string  `yaml:"name"`
	ChType     string  `yaml:"type"`
	EmptyValue *string `yaml:"empty_value"`
	Nullable   bool    `yaml:"nullable"`
}

type columnMapping []map[string]Column

// Table contains information about the table
type Table struct {
	Columns           columnMapping `yaml:"columns"`
	SignColumn        string        `yaml:"sign_column"`
	BufferRowIdColumn string        `yaml:"buffer_row_id"`
	BufferTable       string        `yaml:"buffer_table"`
	BufferSize        int           `yaml:"buffer_size"`
	MainTable         string        `yaml:"main_table"`
	VerColumn         string        `yaml:"ver_column"`
	Engine            tableEngine   `yaml:"engine"`
	MergeThreshold    int           `yaml:"merge_threshold"`
	SkipInitSync      bool          `yaml:"skip_init_sync"`
	SkipBufferTable   bool          `yaml:"skip_buffer_table"`
}

// Config contains config
type Config struct {
	CHConnectionString     string           `yaml:"clickhouse"`
	Pg                     dbConfig         `yaml:"pg"`
	Tables                 map[string]Table `yaml:"tables"`
	InactivityMergeTimeout time.Duration    `yaml:"inactivity_merge_timeout"`
	LsnStateFilepath       string           `yaml:"lsnStateFilepath"`
}

func (t tableEngine) String() string {
	return tableEngines[t]
}

// MarshalYAML ...
func (t tableEngine) MarshalYAML() (interface{}, error) {
	return tableEngines[t], nil
}

// UnmarshalYAML ...
func (t *tableEngine) UnmarshalYAML(unmarshal func(interface{}) error) error {
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

// New instantiates config
func New(filepath string) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close() //TODO: handle err message

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode yaml: %v", err)
	}

	if cfg.Pg.PublicationName == "" {
		return nil, fmt.Errorf("publication name is not specified")
	}

	if cfg.Pg.ReplicationSlotName == "" {
		return nil, fmt.Errorf("replication slot name is not specified")
	}

	connCfg, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, fmt.Errorf("could not parse lib pq env variabels: %v", err)
	}

	if cfg.InactivityMergeTimeout.Seconds() == 0 {
		cfg.InactivityMergeTimeout = defaultInactivityMergeTimeout
	}

	cfg.Pg.ConnConfig = cfg.Pg.ConnConfig.Merge(connCfg)

	return &cfg, nil
}
