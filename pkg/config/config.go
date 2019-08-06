package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

const (
	ApplicationName = "pg2ch"

	defaultInactivityMergeTimeout = time.Minute
	publicSchema                  = "public"
	defaultClickHousePort         = 8123
	defaultClickHouseHost         = "127.0.0.1"
	defaultPostgresPort           = 5432
	defaultPostgresHost           = "127.0.0.1"
	defaultRowIdColumn            = "row_id"
	defaultMaxBufferLength        = 10000
	defaultFlushThreshold         = 100
	defaultSignColumn             = "sign"
	defaultVerColumn              = "ver"
	defaultLsnColumn              = "lsn"
	defaultIsDeletedColumn        = "is_deleted"

	TableLSNKeyPrefix = "table_lsn_"
)

type tableEngine int

const (
	// CollapsingMergeTree represents CollapsingMergeTree table engine
	CollapsingMergeTree tableEngine = iota

	//ReplacingMergeTree represents ReplacingMergeTree table engine
	ReplacingMergeTree

	//MergeTree represents MergeTree table engine
	MergeTree
)

var tableEngines = map[tableEngine]string{
	CollapsingMergeTree: "CollapsingMergeTree",
	ReplacingMergeTree:  "ReplacingMergeTree",
	MergeTree:           "MergeTree",
}

type pgConnConfig struct {
	pgx.ConnConfig `yaml:",inline"`

	ReplicationSlotName string `yaml:"replication_slot_name"`
	PublicationName     string `yaml:"publication_name"`
	Debug               bool   `yaml:"debug"`
}

// PgTableName represents namespaced name
type PgTableName struct {
	SchemaName string
	TableName  string
}

type ChTableName struct {
	DatabaseName string
	TableName    string
}

type coalesceValue []byte

// ColumnProperty describes column properties
type ColumnProperty struct {
	FlattenIstore      bool          `yaml:"flatten_istore"`
	FlattenIstoreMin   int           `yaml:"flatten_istore_min"`
	FlattenIstoreMax   int           `yaml:"flatten_istore_max"`
	IstoreKeysSuffix   string        `yaml:"istore_keys_suffix"`
	IstoreValuesSuffix string        `yaml:"istore_values_suffix"`
	Coalesce           coalesceValue `yaml:"coalesce"`
}

// Table contains information about the table
type Table struct {
	BufferTableRowIdColumn string                    `yaml:"buffer_table_row_id"`
	ChSyncAuxTable         ChTableName               `yaml:"sync_aux_table"`
	ChBufferTable          ChTableName               `yaml:"buffer_table"`
	ChMainTable            ChTableName               `yaml:"main_table"`
	MaxBufferPgDMLs        int                       `yaml:"max_buffer_length"`
	VerColumn              string                    `yaml:"ver_column"`
	IsDeletedColumn        string                    `yaml:"is_deleted_column"`
	SignColumn             string                    `yaml:"sign_column"`
	GenerationColumn       string                    `yaml:"generation_column"`
	Engine                 tableEngine               `yaml:"engine"`
	FlushThreshold         int                       `yaml:"flush_threshold"`
	InitSyncSkip           bool                      `yaml:"init_sync_skip"`
	InitSyncSkipTruncate   bool                      `yaml:"init_sync_skip_truncate"`
	Columns                map[string]string         `yaml:"columns"`
	ColumnProperties       map[string]ColumnProperty `yaml:"column_properties"`
	LsnColumnName          string                    `yaml:"lsn_column_name"`

	PgOID         dbtypes.OID         `yaml:"-"`
	PgTableName   PgTableName         `yaml:"-"`
	TupleColumns  []message.Column    `yaml:"-"` // columns in the order they are in the table
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
	PersStoragePath        string                `yaml:"db_path"`
	RedisBind              string                `yaml:"redis_bind"`
	SyncWorkers            int                   `yaml:"sync_workers"`
	Debug                  bool                  `yaml:"debug"`
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

func (tn *PgTableName) Parse(val string) error {
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

func (cv *coalesceValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string

	if err := unmarshal(&val); err != nil {
		return err
	}

	*cv = []byte(val)

	return nil
}

func (tn *PgTableName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	//TODO: improve parser, use regexp
	var val string

	if err := unmarshal(&val); err != nil {
		return err
	}

	return tn.Parse(val)
}

func (tn PgTableName) MarshalYAML() (interface{}, error) {
	return tn.String(), nil
}

func (tn PgTableName) String() string {
	return tn.NamespacedName()
}

func (tn PgTableName) NamespacedName() string {
	if tn.SchemaName == publicSchema {
		return fmt.Sprintf("%s", tn.TableName)
	}

	return fmt.Sprintf(`%s.%s`, tn.SchemaName, tn.TableName)
}

func (tn PgTableName) KeyName() string {
	return TableLSNKeyPrefix + tn.String()
}

func (tn *PgTableName) ParseKey(key string) error {
	if len(key) < len(TableLSNKeyPrefix) {
		return fmt.Errorf("too short key name")
	}
	if !strings.HasPrefix(key, TableLSNKeyPrefix) {
		return fmt.Errorf("wrong key: %v", key)
	}

	return tn.Parse(key[len(TableLSNKeyPrefix):])
}

// New instantiates config
func New(filepath string) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			log.Printf("could not close config file: %v", err)
		}
	}()

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

	if cfg.PersStoragePath == "" {
		return nil, fmt.Errorf("db_filepath is not set")
	}

	for tblName, tbl := range cfg.Tables {
		newTbl := cfg.Tables[tblName]
		if !tbl.ChBufferTable.IsEmpty() && tbl.ChBufferTable.DatabaseName == "" {
			newTbl.ChBufferTable.DatabaseName = cfg.ClickHouse.Database
		}
		if !tbl.ChMainTable.IsEmpty() && tbl.ChMainTable.DatabaseName == "" {
			newTbl.ChMainTable.DatabaseName = cfg.ClickHouse.Database
		}
		if !tbl.ChSyncAuxTable.IsEmpty() && tbl.ChSyncAuxTable.DatabaseName == "" {
			newTbl.ChSyncAuxTable.DatabaseName = cfg.ClickHouse.Database
		}
		if tbl.FlushThreshold == 0 {
			newTbl.FlushThreshold = defaultFlushThreshold
		}

		cfg.Tables[tblName] = newTbl
	}

	return &cfg, nil
}

func (ct ChTableName) String() string {
	return fmt.Sprintf("%s.%s", ct.DatabaseName, ct.TableName)
}

func (ct ChTableName) IsEmpty() bool {
	return ct.DatabaseName == "" && ct.TableName == ""
}

func (ct *ChTableName) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string

	if err := unmarshal(&val); err != nil {
		return err
	}

	parts := strings.Split(val, ".")
	if ln := len(parts); ln == 2 {
		*ct = ChTableName{
			DatabaseName: parts[0],
			TableName:    parts[1],
		}
	} else if ln == 1 {
		*ct = ChTableName{
			DatabaseName: "",
			TableName:    parts[0],
		}
	} else {
		return fmt.Errorf("too many parts in the table name")
	}

	return nil
}

// UnmarshalYAML ...
func (t *Table) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias Table

	var val alias
	if err := unmarshal(&val); err != nil {
		return err
	}

	if val.BufferTableRowIdColumn == "" {
		val.BufferTableRowIdColumn = defaultRowIdColumn
	}

	if val.SignColumn == "" && val.Engine == CollapsingMergeTree {
		val.SignColumn = defaultSignColumn
	}

	if val.IsDeletedColumn == "" && val.Engine == ReplacingMergeTree {
		val.IsDeletedColumn = defaultIsDeletedColumn
	}

	if val.VerColumn == "" && val.Engine == ReplacingMergeTree {
		val.VerColumn = defaultVerColumn
	}

	if val.MaxBufferPgDMLs == 0 {
		val.MaxBufferPgDMLs = defaultMaxBufferLength
	}

	if val.LsnColumnName == "" {
		val.LsnColumnName = defaultLsnColumn
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

func (c PgColumn) IsIstore() bool {
	return c.BaseType == dbtypes.PgAdjustIstore || c.BaseType == dbtypes.PgAdjustBigIstore
}

func (c PgColumn) IsTime() bool {
	return c.BaseType == dbtypes.PgAdjustAjTime ||
		c.BaseType == dbtypes.PgTimestampWithoutTimeZone ||
		c.BaseType == dbtypes.PgTimestampWithTimeZone ||
		c.BaseType == dbtypes.PgDate ||
		c.BaseType == dbtypes.PgAdjustAjDate
}

func (c Config) Print() {
	fmt.Printf("inactivity flush timeout: %v\n", c.InactivityFlushTimeout)
	fmt.Printf("debug: %t\n", c.Debug)

	for tbl, tblCfg := range c.Tables {
		fmt.Printf("%s: flush threshold: %v buffer threshold: %v\n",
			tbl, tblCfg.FlushThreshold, tblCfg.MaxBufferPgDMLs)
	}
}
