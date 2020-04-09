package tableengines

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/chload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
)

// Generic table is a "parent" struct for all the table engines
const (
	flushBufferAttemptInterval = 5 * time.Second
	maxFlushBufferAttempts     = 1000

	columnDelimiter = '\t'
)

var (
	zeroStr     = []byte("0")
	oneStr      = []byte("1")
	minusOneStr = []byte("-1")
)

type chTuple struct {
	row   message.Row
	extra [][]byte
}

type genericTable struct {
	*sync.Mutex // We set lock on begin and unset it on commit

	ctx          context.Context
	bufferMaxLSN dbtypes.LSN // max lsn in the buffer
	chLoader     chload.CHLoader

	cfg           *config.Table
	chUsedColumns []string
	pgUsedColumns []string
	columnMapping map[string]config.ChColumn // [pg column name]ch column description
	tupleColumns  []message.Column           // Columns description taken from RELATION rep message

	lastCommittedLSN   dbtypes.LSN // last committed finalLSN
	memFlushMutex      *sync.Mutex // memory flush can be triggered from the inactivity merge goroutine as well
	memBufferPgDMLsCnt int         // number of pg DML commands in the current buffer,
	// i.e. 1 update pg dml command => 2 ch inserts: with -1 sign, and +1 sign

	inSync            bool          // protected via table mutex
	syncedRows        uint64        // number of processed rows during the sync
	liveTuples        uint64        // number of live tuples taken from pg_statistics table, i.e. approx num of rows
	syncBuf           *bytes.Buffer // buffer for various things during the sync
	syncLastBatchTime time.Time     //to calculate rate
	auxTblRowID       uint64        // row_id stored in the aux table
	pgTableName       []byte        // table name for the table_name_column_name column

	bulkUploader    bulkupload.BulkUploader // used only during the sync
	syncSnapshotLSN dbtypes.LSN             // LSN of the initial copy snapshot, protected via table mutex
	persStorage     kvstorage.KVStorage     // persistent storage for the processed lsn positions
	logger          *zap.SugaredLogger

	txFinalLSN dbtypes.LSN // finalLSN of the currently processing transaction

	//used only during the sync
	syncPgColumns map[int]*config.PgColumn       // pg columns as they go in the 'select *' statement
	syncColProps  map[int]*config.ColumnProperty // column properties as they go in the 'select *' statement
}

//noinspection GoExportedFuncWithUnexportedType
func NewGenericTable(ctx context.Context, logger *zap.SugaredLogger, persStorage kvstorage.KVStorage,
	loader chload.CHLoader, tblCfg *config.Table) genericTable {
	t := genericTable{
		Mutex:         &sync.Mutex{},
		memFlushMutex: &sync.Mutex{},
		syncBuf:       &bytes.Buffer{},
		ctx:           ctx,
		chLoader:      loader,
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		tupleColumns:  tblCfg.TupleColumns,
		persStorage:   persStorage,
		logger:        logger.With("table", tblCfg.PgTableName.String()),
		pgTableName:   []byte(tblCfg.PgTableName.NamespacedName()),
		syncPgColumns: make(map[int]*config.PgColumn),
		syncColProps:  make(map[int]*config.ColumnProperty),
	}

	pgUsedColID := 0
	for _, pgCol := range t.tupleColumns {
		chCol, ok := tblCfg.ColumnMapping[pgCol.Name]
		if !ok {
			continue
		}

		t.columnMapping[pgCol.Name] = chCol
		t.pgUsedColumns = append(t.pgUsedColumns, pgCol.Name)
		t.syncPgColumns[pgUsedColID] = t.cfg.PgColumns[pgCol.Name]
		t.syncColProps[pgUsedColID] = t.cfg.ColumnProperties[pgCol.Name]
		pgUsedColID++

		if tblCfg.PgColumns[pgCol.Name].IsIstore() {
			if columnCfg, ok := tblCfg.ColumnProperties[pgCol.Name]; ok {
				t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreKeysSuffix))
				t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreValuesSuffix))
			}
		} else {
			t.chUsedColumns = append(t.chUsedColumns, chCol.Name)
		}
	}

	t.chUsedColumns = append(t.chUsedColumns, tblCfg.LsnColumnName)
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.TableNameColumnName)

	return t
}

func (t *genericTable) truncateTable(tableName config.ChTableName) error {
	t.logger.Infof("truncating %q table", tableName)

	return t.chLoader.Exec(fmt.Sprintf("truncate table %s.%s", tableName.DatabaseName, tableName.TableName))
}

func (t *genericTable) createAuxTable(tableName config.ChTableName,
	like config.ChTableName) error {

	t.logger.Infof("creating %q table like %q", tableName, like)

	err := t.chLoader.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[3]s ENGINE=MergeTree()
		ORDER BY %[1]s PARTITION BY %[2]s as
			SELECT *, toUInt64(0) as row_id FROM %[4]s LIMIT 0;`,
		t.cfg.RowIDColumnName, t.cfg.TableNameColumnName,
		tableName.String(), like.String()))
	if err != nil {
		return err
	}

	return nil
}

func (t *genericTable) dropTablePartition(tableName config.ChTableName, partitionName string) error {
	t.logger.Infof("dropping %q partiton of the %q table", partitionName, tableName)

	return t.chLoader.Exec(fmt.Sprintf("alter table %s drop partition '%s'",
		tableName.String(), partitionName))
}

func (t *genericTable) advanceProcessedLSN() error {
	if t.bufferMaxLSN >= t.lastCommittedLSN {
		return t.saveLSN(t.lastCommittedLSN)
	}

	return nil
}

func (t *genericTable) writeRow(tuples ...chTuple) error {
	if len(tuples) == 0 {
		return nil
	}

	for _, tuple := range tuples {
		if t.inSync {
			// writing to the aux table
			if err := t.writeRowToBuffer(
				t.chLoader, // we use the same buffer as during the normal operation
				tuple.row,
				t.txFinalLSN,
				t.pgTableName,
				append(tuple.extra, []byte(strconv.FormatUint(t.auxTblRowID, 10)))...); err != nil {
				return err
			}
		} else {
			if err := t.writeRowToBuffer(t.chLoader, tuple.row, t.txFinalLSN, t.pgTableName, tuple.extra...); err != nil {
				return err
			}
		}
	}

	if err := t.chLoader.WriteByte('\n'); err != nil {
		return err
	}

	if t.bufferMaxLSN.IsValid() {
		if t.txFinalLSN > t.bufferMaxLSN {
			t.bufferMaxLSN = t.txFinalLSN
		}
	} else {
		t.bufferMaxLSN = t.txFinalLSN
	}

	t.memBufferPgDMLsCnt++

	return nil
}

func (t *genericTable) flushBuffer() error {
	t.memFlushMutex.Lock()
	defer t.memFlushMutex.Unlock()

	return utils.Try(t.ctx, maxFlushBufferAttempts, flushBufferAttemptInterval, func() error {
		t.logger.Debugf("Flush buffer: started")
		if t.memBufferPgDMLsCnt == 0 {
			t.logger.Debugf("Flush buffer: mem buffer is empty")
			return nil
		}

		if t.inSync {
			t.logger.Debugf("Flush buffer: flushing to the aux table %s (cols: %v)", t.cfg.ChSyncAuxTable, t.syncAuxTableColumns())
			if err := t.chLoader.Flush(t.cfg.ChSyncAuxTable, t.syncAuxTableColumns()); err != nil {
				return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChSyncAuxTable, err)
			}
		} else {
			t.logger.Debugf("Flush buffer: flushing to the main table %s", t.cfg.ChMainTable)
			if err := t.chLoader.Flush(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
				return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChMainTable, err)
			}

			if err := t.advanceProcessedLSN(); err != nil {
				return fmt.Errorf("could not advance processed lsn: %v", err)
			}
		}

		t.logger.Debugf("Flush buffer: finished")
		t.memBufferPgDMLsCnt = 0
		t.bufferMaxLSN = dbtypes.InvalidLSN

		return nil
	})
}

func (t *genericTable) saveLSN(lsn dbtypes.LSN) error {
	t.logger.Debugf("lsn %s saved", lsn)

	return t.persStorage.WriteLSN(t.cfg.PgTableName.KeyName(), lsn)
}

//Flush flushes data from buffer table to the main one
func (t *genericTable) Flush() error {
	t.logger.Debugf("Flush: trying to acquire table lock")
	t.Lock()
	t.logger.Debugf("Flush: table lock acquired")
	defer t.Unlock()

	return t.flushBuffer()
}

func (t *genericTable) writeRowToBuffer(buf utils.Writer, row message.Row, lsn dbtypes.LSN, tableName []byte, extras ...[]byte) error {
	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if colId > 0 {
			if err := buf.WriteByte(columnDelimiter); err != nil {
				return err
			}
		}

		if err := chutils.ConvertColumn(buf, t.cfg.PgColumns[col.Name], row[colId], t.cfg.ColumnProperties[col.Name]); err != nil {
			return err
		}
	}

	if err := buf.WriteByte(columnDelimiter); err != nil {
		return err
	}
	if _, err := buf.Write(lsn.Decimal()); err != nil {
		return err
	}

	if err := buf.WriteByte(columnDelimiter); err != nil {
		return err
	}
	if _, err := buf.Write(tableName); err != nil {
		return err
	}

	for _, extra := range extras {
		if err := buf.WriteByte(columnDelimiter); err != nil {
			return err
		}

		if _, err := buf.Write(extra); err != nil {
			return err
		}
	}

	return nil
}

// Truncate truncates main and buffer(if used) tables
func (t *genericTable) Truncate() error {
	t.memBufferPgDMLsCnt = 0

	if err := t.truncateTable(t.cfg.ChMainTable); err != nil {
		return err
	}

	return nil
}

// Init performs initialization and deletes all the dirty data, if any
func (t *genericTable) Init(lastFinalLSN dbtypes.LSN) error {
	if lastFinalLSN.IsValid() {
		sql := fmt.Sprintf("alter table %s delete where lsn > %d and table_name = '%s'",
			t.cfg.ChMainTable.NamespacedName(), uint64(lastFinalLSN), t.cfg.PgTableName.NamespacedName())

		t.logger.Infow("deleting all the dirty data", "sql", sql)
		if err := t.chLoader.Exec(sql); err != nil {
			return fmt.Errorf("could not delete dirty tuples from %s table: %v", t.cfg.ChMainTable, err)
		}
	}

	return nil
}

func (t *genericTable) compareRows(a, b message.Row) (bool, bool) {
	equal := true
	keyColumnChanged := false
	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if a[colId].Kind != message.TupleNull {
			if !bytes.Equal(a[colId].Value, b[colId].Value) {
				equal = false
				if col.IsKey {
					keyColumnChanged = true
				}
			}
		} else if b[colId].Kind == message.TupleNull {
			equal = false
			if col.IsKey { // should never happen
				keyColumnChanged = true
			}
		} else if a[colId].Kind != b[colId].Kind {
			equal = false
			if col.IsKey {
				keyColumnChanged = true
			}
		}
	}

	return equal, keyColumnChanged
}

func (t *genericTable) Begin(finalLSN dbtypes.LSN) error {
	t.logger.Debugf("Begin: trying to acquire table lock")
	t.Lock()
	t.logger.Debugf("Begin: table lock acquired")
	t.txFinalLSN = finalLSN

	return nil
}

func (t *genericTable) Commit() error {
	t.lastCommittedLSN = t.txFinalLSN

	t.logger.Debugf("Commit: finished")

	t.Unlock()
	return nil
}

func (t *genericTable) String() string {
	return t.cfg.PgTableName.String()
}
