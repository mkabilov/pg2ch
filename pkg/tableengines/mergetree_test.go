package tableengines

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/jackc/pgx"
	"go.uber.org/zap"
	"gopkg.in/djherbis/buffer.v1"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
)

type mockBulkUploader struct {
	buf *bytes.Buffer
}

type mockChLoader struct {
	buf       *bytes.Buffer
	lastcmnds []string
}

type mockTxExecutor struct {
	buf       *bytes.Buffer
	lastcmnds []string
}

func (m *mockBulkUploader) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}

func (m *mockBulkUploader) Init(buffer.BufferAt) error {
	return nil
}

func (m *mockBulkUploader) Finish() error {
	return nil
}

func (m *mockBulkUploader) BulkUpload(name config.ChTableName, columns []string) error {
	return nil
}

func (m *mockBulkUploader) WriteByte(p byte) error {
	_, err := m.buf.Write([]byte{p})

	return err
}

func newChLoaderMock() *mockChLoader {
	return &mockChLoader{
		buf:       &bytes.Buffer{},
		lastcmnds: make([]string, 0),
	}
}

func newMockBulkUploader() *mockBulkUploader {
	return &mockBulkUploader{
		buf: &bytes.Buffer{},
	}
}

func newMockTxExecutor() *mockTxExecutor {
	return &mockTxExecutor{
		buf: &bytes.Buffer{},
	}
}

func (m *mockChLoader) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}

func (m *mockChLoader) WriteByte(p byte) error {
	_, err := m.buf.Write([]byte{p})

	return err
}

func (m *mockChLoader) Flush(tableName config.ChTableName, columns []string) error {
	return nil
}

func (m *mockChLoader) Exec(str string) error {
	m.lastcmnds = append(m.lastcmnds, str)
	return nil
}

func (m *mockChLoader) lastCommands() []string {
	return m.lastcmnds
}

func (m *mockChLoader) resetLastCommands() {
	m.lastcmnds = make([]string, 0)
}

func (m *mockChLoader) Query(string) ([][]string, error) {
	return nil, nil
}

func (m *mockTxExecutor) CopyToWriter(w io.Writer, sql string, args ...interface{}) (pgx.CommandTag, error) {
	return pgx.CommandTag("SELECT 10"), nil
}

func TestNewMergeTree(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tmpDir, err := ioutil.TempDir("", "mergetree")
	if err != nil {
		t.Fatalf("unexpected error while creating temp dir: %v", err)
	}

	kvStrge, err := kvstorage.New("diskv", tmpDir)
	if err != nil {
		t.Fatalf("unexpected error while creating persistent storage: %v", err)
	}

	chLoader := newChLoaderMock()

	var tblCfg = &config.Table{
		ChMainTable: config.ChTableName{
			DatabaseName: "default",
			TableName:    "my_table_main",
		},
		ChSyncAuxTable: config.ChTableName{
			DatabaseName: "default",
			TableName:    "my_table_aux",
		},
		IsDeletedColumn:      "is_deleted",
		SignColumn:           "sign",
		RowIDColumnName:      "row_id",
		TableNameColumnName:  "table_name",
		LsnColumnName:        "lsn",
		Engine:               config.MergeTree,
		BufferSize:           10,
		InitSyncSkip:         false,
		InitSyncSkipTruncate: false,
		Columns:              nil,
		ColumnProperties: map[string]*config.ColumnProperty{
			"punchcard": {
				IstoreKeysSuffix:   "keys",
				IstoreValuesSuffix: "values",
			},
		},
		PgTableName: config.PgTableName{
			SchemaName: "public",
			TableName:  "my_table",
		},
		PgColumns: map[string]*config.PgColumn{
			"id": {
				Column: config.Column{
					BaseType:   dbtypes.PgInteger,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				PkCol: 1,
			},
			"value": {
				Column: config.Column{
					BaseType:   dbtypes.PgText,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				PkCol: 0,
			},
			"punchcard": {
				Column: config.Column{
					BaseType:   dbtypes.PgAdjustIstore,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				PkCol: 0,
			},
		},
		TupleColumns: []message.Column{
			{
				IsKey:   true,
				Name:    "id",
				TypeOID: 23,
				Mode:    0,
			},
			{
				IsKey:   false,
				Name:    "value",
				TypeOID: 25,
				Mode:    0,
			},
			{
				IsKey:   false,
				Name:    "punchcard",
				TypeOID: 12345,
				Mode:    0,
			},
		},
		ColumnMapping: map[string]config.ChColumn{
			"id": {
				Column: config.Column{
					BaseType:   dbtypes.PgInteger,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				Name: "id",
			},
			"value": {
				Column: config.Column{
					BaseType:   dbtypes.PgText,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				Name: "value",
			},
			"punchcard": {
				Column: config.Column{
					BaseType:   dbtypes.PgAdjustIstore,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
				Name: "punchcard",
			},
		},
	}
	snapshotLSN := dbtypes.LSN(1234)

	genTbl := NewGenericTable(context.Background(), logger, kvStrge, chLoader, tblCfg)
	tbl := NewMergeTree(genTbl, tblCfg)

	if expected := []string{"id", "value", "punchcard"}; !reflect.DeepEqual(tbl.pgUsedColumns, expected) {
		t.Fatalf("expected: %v, got: %v", expected, tbl.pgUsedColumns)
	}

	if err := tbl.Init(snapshotLSN); err != nil {
		t.Fatalf("unexpected during the init error: %v", err)
	}
	expectedCommands := []string{
		"truncate table default.my_table_aux",
		"alter table default.my_table_main delete where lsn > 1234 and table_name = 'my_table'",
	}
	if got := chLoader.lastCommands(); !reflect.DeepEqual(got, expectedCommands) {
		t.Fatalf("expected commands: %v, got: %v", expectedCommands, got)
	}
	chLoader.resetLastCommands()
}
