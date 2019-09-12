package config

import (
	"compress/flate"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"go.uber.org/zap/zapcore"
)

const (
	ApplicationName = "pg2ch"

	publicSchema               = "public"
	defaultClickHousePort      = 8123
	defaultClickHouseHost      = "127.0.0.1"
	defaultPostgresHost        = "127.0.0.1"
	DefaultRowIDColumn         = "row_id"
	defaultBufferSize          = 1000
	defaultGzipBufferSize      = 1000
	defaultSignColumn          = "sign"
	defaultLsnColumn           = "lsn"
	defaultIsDeletedColumn     = "is_deleted"
	defaultTableNameColumnName = "table_name"
	defaultPerStorageType      = "diskv"

	TableLSNKeyPrefix = "table_lsn_"
)

/* these variables could be changed in tests */
var (
	DefaultPostgresPort           uint16 = 5432
	DefaultInactivityMergeTimeout        = time.Minute
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

var (
	tableEngines = map[tableEngine]string{
		CollapsingMergeTree: "CollapsingMergeTree",
		ReplacingMergeTree:  "ReplacingMergeTree",
		MergeTree:           "MergeTree",
	}

	compressionLevels = map[string]int{
		"no":              flate.NoCompression,
		"bestspeed":       flate.BestSpeed,
		"bestcompression": flate.BestCompression,
		"default":         flate.DefaultCompression,
		"huffmanonly":     flate.HuffmanOnly,
	}
)

type GzipComprLevel int

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
	IstoreKeysSuffix   string        `yaml:"istore_keys_suffix"`
	IstoreValuesSuffix string        `yaml:"istore_values_suffix"`
	Coalesce           coalesceValue `yaml:"coalesce"`
}

// Table contains information about the table
type Table struct {
	ChSyncAuxTable       ChTableName               `yaml:"sync_aux_table"`
	ChMainTable          ChTableName               `yaml:"main_table"`
	IsDeletedColumn      string                    `yaml:"is_deleted_column"`
	SignColumn           string                    `yaml:"sign_column"`
	RowIDColumnName      string                    `yaml:"row_id_column"`
	TableNameColumnName  string                    `yaml:"table_name_column_name"`
	Engine               tableEngine               `yaml:"engine"`
	BufferSize           int                       `yaml:"max_buffer_length"`
	InitSyncSkip         bool                      `yaml:"init_sync_skip"`
	InitSyncSkipTruncate bool                      `yaml:"init_sync_skip_truncate"`
	Columns              map[string]string         `yaml:"columns"`
	ColumnProperties     map[string]ColumnProperty `yaml:"column_properties"`
	LsnColumnName        string                    `yaml:"lsn_column_name"`

	PgOID         dbtypes.OID         `yaml:"-"`
	PgTableName   PgTableName         `yaml:"-"`
	TupleColumns  []message.Column    `yaml:"-"` // columns in the order they are in the table
	PgColumns     map[string]PgColumn `yaml:"-"` // map of pg column structs
	ColumnMapping map[string]ChColumn `yaml:"-"` // mapping pg column -> ch column
}

type CHConnConfig struct {
	Host            string            `yaml:"host"`
	Port            uint32            `yaml:"port"`
	Database        string            `yaml:"database"`
	User            string            `yaml:"username"`
	Password        string            `yaml:"password"`
	Params          map[string]string `yaml:"params"`
	GzipBufSize     int               `yaml:"gzip_buffer_size"`
	GzipCompression GzipComprLevel    `yaml:"gzip_compression"`
}

// Config contains config
type Config struct {
	ClickHouse             CHConnConfig           `yaml:"clickhouse"`
	Postgres               pgConnConfig           `yaml:"postgres"`
	Tables                 map[PgTableName]*Table `yaml:"tables"`
	InactivityFlushTimeout time.Duration          `yaml:"inactivity_flush_timeout"`
	PersStoragePath        string                 `yaml:"db_path"`
	PersStorageType        string                 `yaml:"db_type"`
	RedisBind              string                 `yaml:"redis_bind"`
	PprofBind              string                 `yaml:"pprof_bind"`
	SyncWorkers            int                    `yaml:"sync_workers"`
	LogLevel               zapcore.Level          `yaml:"loglevel"`
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

func (gc *GzipComprLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string

	if err := unmarshal(&str); err != nil {
		return err
	}

	if str == "" {
		*gc = GzipComprLevel(flate.NoCompression)

		return nil
	}

	val, ok := compressionLevels[strings.ToLower(str)]
	if !ok {
		return fmt.Errorf("unknown compression level %q", str)
	}
	*gc = GzipComprLevel(val)

	return nil
}

func (gc GzipComprLevel) String() string {
	for k, v := range compressionLevels {
		if v == int(gc) {
			return k
		}
	}

	return "unknown" //should never happen
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
	cfg := &Config{}

	fp, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			log.Printf("could not close config file: %v", err)
		}
	}()

	if err := yaml.NewDecoder(fp).Decode(cfg); err != nil {
		return nil, fmt.Errorf("could not decode yaml: %v", err)
	}

	if len(cfg.Postgres.PublicationName) == 0 {
		return nil, fmt.Errorf("publication name is not specified")
	}

	if len(cfg.Postgres.ReplicationSlotName) == 0 {
		return nil, fmt.Errorf("replication slot name is not specified")
	}

	if cfg.InactivityFlushTimeout.Seconds() == 0 {
		cfg.InactivityFlushTimeout = DefaultInactivityMergeTimeout
	}

	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = DefaultPostgresPort
	}

	if len(cfg.Postgres.Host) == 0 {
		cfg.Postgres.Host = defaultPostgresHost
	}

	if cfg.ClickHouse.Port == 0 {
		cfg.ClickHouse.Port = defaultClickHousePort
	}

	if len(cfg.ClickHouse.Host) == 0 {
		cfg.ClickHouse.Host = defaultClickHouseHost
	}

	if len(cfg.PersStoragePath) == 0 {
		return nil, fmt.Errorf("db_filepath is not set")
	}

	if len(cfg.PersStorageType) == 0 {
		cfg.PersStorageType = defaultPerStorageType
	}

	if cfg.SyncWorkers == 0 {
		cfg.SyncWorkers = runtime.NumCPU()
	}

	for _, tbl := range cfg.Tables {
		if !tbl.ChMainTable.IsEmpty() && tbl.ChMainTable.DatabaseName == "" {
			tbl.ChMainTable.DatabaseName = cfg.ClickHouse.Database
		}
		if !tbl.ChSyncAuxTable.IsEmpty() && tbl.ChSyncAuxTable.DatabaseName == "" {
			tbl.ChSyncAuxTable.DatabaseName = cfg.ClickHouse.Database
		}
		if tbl.BufferSize == 0 {
			tbl.BufferSize = defaultBufferSize
		}
	}

	if cfg.ClickHouse.GzipBufSize == 0 && cfg.ClickHouse.GzipCompression != flate.NoCompression {
		cfg.ClickHouse.GzipBufSize = defaultGzipBufferSize
	}

	return cfg, nil
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

	if val.RowIDColumnName == "" {
		val.RowIDColumnName = DefaultRowIDColumn
	}
	if val.SignColumn == "" && val.Engine == CollapsingMergeTree {
		val.SignColumn = defaultSignColumn
	}

	if val.IsDeletedColumn == "" && val.Engine == ReplacingMergeTree {
		val.IsDeletedColumn = defaultIsDeletedColumn
	}

	if val.LsnColumnName == "" {
		val.LsnColumnName = defaultLsnColumn
	}

	if val.TableNameColumnName == "" {
		val.TableNameColumnName = defaultTableNameColumnName
	}

	*t = Table(val)

	return nil
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
	fmt.Fprintf(os.Stderr, "inactivity flush timeout: %v\n", c.InactivityFlushTimeout)
	fmt.Fprintf(os.Stderr, "logging level: %s\n", c.LogLevel)
	fmt.Fprintf(os.Stderr, "number of sync workers: %d\n", c.SyncWorkers)
	fmt.Fprintf(os.Stderr, "clickhouse gzip compression: %v\n", c.ClickHouse.GzipCompression)
	if c.ClickHouse.GzipCompression != flate.NoCompression {
		fmt.Fprintf(os.Stderr, "clickhouse gzip buffer size: %v\n", c.ClickHouse.GzipBufSize)
	}

	targetTables := make(map[string][]string)
	for pgTable, tblCfg := range c.Tables {
		chTableStr := tblCfg.ChMainTable.String()
		pgTableStr := fmt.Sprintf("pg: %s", pgTable.String())
		if !tblCfg.ChSyncAuxTable.IsEmpty() {
			pgTableStr += fmt.Sprintf(" aux: %s", tblCfg.ChSyncAuxTable.String())
		}
		pgTableStr += fmt.Sprintf(" flush threshold: %v", tblCfg.BufferSize)
		if _, ok := targetTables[chTableStr]; ok {
			targetTables[chTableStr] = append(targetTables[chTableStr], pgTableStr)
		} else {
			targetTables[chTableStr] = []string{pgTableStr}
		}
	}

	fmt.Fprintf(os.Stderr, "clickhouse table - postgres table(s) mapping\n")
	for chTable, pgTables := range targetTables {
		fmt.Fprintf(os.Stderr, "%s:\n", chTable)
		sort.Strings(pgTables)
		for _, pgTable := range pgTables {
			fmt.Fprintf(os.Stderr, "\t%s\n", pgTable)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}
}
