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

// NamespacedName represents namespaced name
type NamespacedName struct {
	SchemaName string
	TableName  string
}

// Table contains information about the table
type Table struct {
	BufferTableRowIdColumn string            `yaml:"buffer_table_row_id"`
	BufferTable            string            `yaml:"buffer_table"`
	MemoryBufferSize       int               `yaml:"memory_buffer_size"`
	MainTable              string            `yaml:"main_table"`
	VerColumn              string            `yaml:"ver_column"`
	SignColumn             string            `yaml:"sign_column"`
	Engine                 tableEngine       `yaml:"engine"`
	FlushThreshold         int               `yaml:"flush_threshold"`
	SkipInitSync           bool              `yaml:"skip_init_sync"`
	SkipBufferTable        bool              `yaml:"skip_buffer_table"`
	EmptyValues            map[string]string `yaml:"empty_values"`
	Columns                map[string]string `yaml:"columns"`

	PgColumns     []string            `yaml:"-"`
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
	ClickHouse             chConnConfig             `yaml:"clickhouse"`
	Postgres               pgConnConfig             `yaml:"postgres"`
	Tables                 map[NamespacedName]Table `yaml:"tables"`
	InactivityFlushTimeout time.Duration            `yaml:"inactivity_flush_timeout"`
	LsnStateFilepath       string                   `yaml:"lsnStateFilepath"`
}

// ChColumn describes ClickHouse column
type ChColumn struct {
	Name       string
	BaseType   string
	IsArray    bool
	IsNullable bool
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

func (tn *NamespacedName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string

	if err := unmarshal(&val); err != nil {
		return err
	}

	parts := strings.Split(val, ".")
	if ln := len(parts); ln == 2 {
		*tn = NamespacedName{
			SchemaName: parts[0],
			TableName:  parts[1],
		}
	} else if ln == 1 {
		*tn = NamespacedName{
			SchemaName: publicSchema,
			TableName:  parts[0],
		}
	} else {
		return fmt.Errorf("invalid table name: %q", val)
	}

	return nil
}

func (tn *NamespacedName) String() string {
	if tn.SchemaName == publicSchema {
		return fmt.Sprintf("%q", tn.TableName)
	}

	return fmt.Sprintf(`%q.%q`, tn.SchemaName, tn.TableName)
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

	if cfg.ClickHouse.Port == 0 {
		cfg.ClickHouse.Port = defaultClickHousePort
	}

	if cfg.ClickHouse.Host == "" {
		cfg.ClickHouse.Host = defaultClickHouseHost
	}

	return &cfg, nil
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
