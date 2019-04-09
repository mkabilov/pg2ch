package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"
)

const (
	defaultInactivityMergeTimeout = time.Minute
	publicSchema                  = "public"
	defaultClickHousePort         = 9000
	defaultClickHouseHost         = "127.0.0.1"
	defaultPostgresPort           = 5432
	defaultPostgresHost           = "127.0.0.1"
	defaultRowIdColumn            = "row_id"
	defaultMaxBufferLength        = 1000
	defaultSignColumn             = "sign"
	defaultVerColumn              = "ver"
)

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

type pgConnConfig struct {
	pgx.ConnConfig `yaml:",inline"`

	ReplicationSlotName string `yaml:"replication_slot_name"`
	PublicationName     string `yaml:"publication_name"`
}

// PgTableName represents namespaced name
type PgTableName struct {
	SchemaName string
	TableName  string
}

// Table contains information about the table
type Table struct {
	BufferTableRowIdColumn  string            `yaml:"buffer_table_row_id"`
	ChBufferTable           string            `yaml:"buffer_table"`
	ChMainTable             string            `yaml:"main_table"`
	MaxBufferLength         int               `yaml:"max_buffer_length"`
	VerColumn               string            `yaml:"ver_column"`
	SignColumn              string            `yaml:"sign_column"`
	Engine                  tableEngine       `yaml:"engine"`
	FlushThreshold          int               `yaml:"flush_threshold"`
	InitSyncSkip            bool              `yaml:"init_sync_skip"`
	InitSyncSkipBufferTable bool              `yaml:"init_sync_skip_buffer_table"`
	EmptyValues             map[string]string `yaml:"empty_values"`
	Columns                 map[string]string `yaml:"columns"`

	PgTableName   PgTableName         `yaml:"-"`
	TupleColumns  []string            `yaml:"-"` // columns in the order they are in the table
	PgColumns     map[string]PgColumn `yaml:"-"`
	ColumnMapping map[string]ChColumn `yaml:"-"`
}

type chConnConfig struct {
	Host     string            `yaml:"host"`
	Port     uint32            `yaml:"port"`
	Database string            `yaml:"database"`
	User     string            `yaml:"username"`
	Password string            `yaml:"password"`
	Params   map[string]string `yaml:"params"`
}

// Config contains config
type Config struct {
	ClickHouse             chConnConfig          `yaml:"clickhouse"`
	Postgres               pgConnConfig          `yaml:"postgres"`
	Tables                 map[PgTableName]Table `yaml:"tables"`
	InactivityFlushTimeout time.Duration         `yaml:"inactivity_flush_timeout"`
	LsnStateFilepath       string                `yaml:"lsnStateFilepath"`
}

type Column struct {
	BaseType   string
	IsArray    bool
	IsNullable bool
	Ext        []int
}

type PgColumn struct {
	Column
	PkCol int
}

// ChColumn describes ClickHouse column
type ChColumn struct {
	Column
	Name string
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

func (tn *PgTableName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	//TODO: improve parser, use regexp
	var val string

	if err := unmarshal(&val); err != nil {
		return err
	}

	parts := strings.Split(val, ".")
	if ln := len(parts); ln == 2 {
		*tn = PgTableName{
			SchemaName: parts[0],
			TableName:  parts[1],
		}
	} else if ln == 1 {
		*tn = PgTableName{
			SchemaName: publicSchema,
			TableName:  parts[0],
		}
	} else {
		return fmt.Errorf("invalid table name: %q", val)
	}

	return nil
}

func (tn PgTableName) MarshalYAML() (interface{}, error) {
	return tn.String(), nil
}

func (tn *PgTableName) String() string {
	if tn.SchemaName == publicSchema {
		return fmt.Sprintf("%s", tn.TableName)
	}

	return fmt.Sprintf(`%s.%s`, tn.SchemaName, tn.TableName)
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

	if cfg.Postgres.PublicationName == "" {
		return nil, fmt.Errorf("publication name is not specified")
	}

	if cfg.Postgres.ReplicationSlotName == "" {
		return nil, fmt.Errorf("replication slot name is not specified")
	}

	connCfg, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, fmt.Errorf("could not parse lib pq env variabels: %v", err)
	}

	if cfg.InactivityFlushTimeout.Seconds() == 0 {
		cfg.InactivityFlushTimeout = defaultInactivityMergeTimeout
	}

	cfg.Postgres.ConnConfig = cfg.Postgres.ConnConfig.Merge(connCfg)

	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = defaultPostgresPort
	}

	if cfg.Postgres.Host == "" {
		cfg.Postgres.Host = defaultPostgresHost
	}

	if cfg.ClickHouse.Port == 0 {
		cfg.ClickHouse.Port = defaultClickHousePort
	}

	if cfg.ClickHouse.Host == "" {
		cfg.ClickHouse.Host = defaultClickHouseHost
	}

	return &cfg, nil
}

// UnmarshalYAML ...
func (t *Table) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias Table

	var val alias
	if err := unmarshal(&val); err != nil {
		return err
	}

	if val.ChBufferTable != "" && val.BufferTableRowIdColumn == "" {
		val.BufferTableRowIdColumn = defaultRowIdColumn
	}

	if val.SignColumn == "" && (val.Engine == CollapsingMergeTree || val.Engine == VersionedCollapsingMergeTree) {
		val.SignColumn = defaultSignColumn
	}

	if val.VerColumn == "" && (val.Engine == ReplacingMergeTree || val.Engine == VersionedCollapsingMergeTree) {
		val.VerColumn = defaultVerColumn
	}

	if val.MaxBufferLength == 0 {
		val.MaxBufferLength = defaultMaxBufferLength
	}

	*t = Table(val)

	return nil
}

// ConnectionString returns clickhouse connection string
func (c *chConnConfig) ConnectionString() string {
	connStr := url.Values{}

	connStr.Add("username", c.User)
	connStr.Add("password", c.Password)
	connStr.Add("database", c.Database)

	for param, value := range c.Params {
		connStr.Add(param, value)
	}

	return fmt.Sprintf("tcp://%s:%d?%s", c.Host, c.Port, connStr.Encode())
}
