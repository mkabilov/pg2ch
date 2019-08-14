package tableengines

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/peterbourgon/diskv"
	"go.uber.org/zap"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/chload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

// Generic table is a "parent" struct for all the table engines
const (
	attemptInterval = time.Second
	maxAttempts     = 100

	columnDelimiter = '\t'
)

var (
	zeroStr     = []byte("0")
	oneStr      = []byte("1")
	minusOneStr = []byte("-1")
)

type chTuple []byte
type chTuples []chTuple

type genericTable struct {
	*sync.Mutex

	ctx      context.Context
	chLoader chload.CHLoader

	cfg           *config.Table
	chUsedColumns []string
	pgUsedColumns []string
	columnMapping map[string]config.ChColumn // [pg column name]ch column description
	tupleColumns  []message.Column           // Columns description taken from RELATION rep message

	memBufferPgDMLsCnt int    // number of pg DML commands in the current buffer, i.e. 1 update pg dml command => 2 ch inserts
	memBufferRowId     uint64 // row id in the buffer, will be used as a sorting column while flushing to the main table
	memBufferFlushCnt  int    // number of flushed memory buffers

	tblBufferFlushQueries []string
	generationID          *uint64

	inSync            bool // protected via table mutex
	syncedRows        uint64
	rowsToSync        uint64
	syncLastBatchTime time.Time //to calculate rate
	auxTblRowID       uint64

	bulkUploader    bulkupload.BulkUploader
	syncSnapshotLSN dbtypes.LSN // LSN of the initial copy snapshot, protected via table mutex
	persStorage     *diskv.Diskv
	logger          *zap.SugaredLogger

	txFinalLSN dbtypes.LSN
}

func NewGenericTable(ctx context.Context, logger *zap.SugaredLogger, persStorage *diskv.Diskv,
	conn *chutils.CHConn, tblCfg *config.Table, genID *uint64) genericTable {
	t := genericTable{
		Mutex:         &sync.Mutex{},
		ctx:           ctx,
		chLoader:      chload.New(conn),
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		tupleColumns:  tblCfg.TupleColumns,
		generationID:  genID,
		bulkUploader:  bulkupload.New(conn, gzipFlushCount),
		persStorage:   persStorage,
		logger:        logger.With("table", tblCfg.PgTableName.String()),
	}

	for _, pgCol := range t.tupleColumns {
		chCol, ok := tblCfg.ColumnMapping[pgCol.Name]
		if !ok {
			continue
		}

		t.columnMapping[pgCol.Name] = chCol
		t.pgUsedColumns = append(t.pgUsedColumns, pgCol.Name)

		if tblCfg.PgColumns[pgCol.Name].IsIstore() {
			if columnCfg, ok := tblCfg.ColumnProperties[pgCol.Name]; ok {
				t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreKeysSuffix))
				t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreValuesSuffix))
			}
		} else {
			t.chUsedColumns = append(t.chUsedColumns, chCol.Name)
		}
	}

	if tblCfg.GenerationColumn != "" {
		t.chUsedColumns = append(t.chUsedColumns, tblCfg.GenerationColumn)
	}

	return t
}

func (t *genericTable) truncateTable(tableName config.ChTableName) error {
	t.logger.Debugf("truncating %q table", tableName)
	if err := t.chLoader.Exec(fmt.Sprintf("truncate table %s.%s", tableName.DatabaseName, tableName.TableName)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) processChTuples(set chTuples) (tblFlushIsNeeded bool, err error) {
	if set != nil {
		for _, row := range set {
			if t.inSync {
				row = append(row, '\t')
				row = append(row, strconv.FormatUint(t.auxTblRowID, 10)...)
				row = append(row, '\t')
				row = append(row, t.txFinalLSN.StrBytes()...)

				t.auxTblRowID++
			} else if t.txFinalLSN < t.syncSnapshotLSN {
				return false, nil
			} else if !t.cfg.ChBufferTable.IsEmpty() {
				row = append(row, '\t')
				row = append(row, strconv.FormatUint(t.memBufferRowId, 10)...)
			}

			t.logger.Debugw("buffer write", "line", row)
			if err := t.chLoader.BufferWriteLine(row); err != nil {
				return false, err
			}

			t.memBufferRowId++
		}

		t.memBufferPgDMLsCnt++
	}

	if t.memBufferPgDMLsCnt >= t.cfg.MaxBufferPgDMLs {
		if err := t.flushMemBuffer(); err != nil {
			return false, err
		}
	}

	if t.cfg.ChBufferTable.IsEmpty() {
		return false, nil
	}

	return t.memBufferFlushCnt >= t.cfg.FlushThreshold, nil
}

func (t *genericTable) flushMemBuffer() error {
	return utils.Try(t.ctx, maxAttempts, attemptInterval, t.attemptFlushMemBuffer)
}

// flush from memory to the buffer/main table
func (t *genericTable) attemptFlushMemBuffer() error {
	t.logger.Debugf("Memory buffer flush: started")
	if t.memBufferPgDMLsCnt == 0 {
		t.logger.Debugf("Memory buffer flush: mem buffer is empty")
		return nil
	}

	if t.inSync {
		t.logger.Debugf("Memory buffer flush: in sync buffer flush to %s", t.cfg.ChSyncAuxTable)
		if err := t.chLoader.BufferFlush(t.cfg.ChSyncAuxTable, t.syncAuxTableColumns()); err != nil {
			return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChSyncAuxTable, err)
		}
	} else {
		if !t.cfg.ChBufferTable.IsEmpty() {
			t.logger.Debugf("Memory buffer flush: not in sync flushing to buffer %s table", t.cfg.ChBufferTable)
			if err := t.chLoader.BufferFlush(t.cfg.ChBufferTable, append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn)); err != nil {
				return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChBufferTable, err)
			}
		} else {
			t.logger.Debugf("Memory buffer flush: not in sync flushing to main table")
			if err := t.chLoader.BufferFlush(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
				return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChMainTable, err)
			}
		}
	}

	t.logger.Debugf("Memory buffer flush: finished")
	t.memBufferPgDMLsCnt = 0
	t.memBufferFlushCnt++

	return nil
}

// flush from buffer table to main table
func (t *genericTable) attemptFlushTblBuffer() error {
	t.logger.Debugf("Table buffer flush: started")
	for _, query := range t.tblBufferFlushQueries {
		t.logger.Debugf("Table buffer flush: executing: %v", query)
		if err := t.chLoader.Exec(query); err != nil {
			return err
		}
	}

	t.memBufferFlushCnt = 0
	t.memBufferRowId = 0
	if err := t.truncateTable(t.cfg.ChBufferTable); err != nil {
		return fmt.Errorf("could not truncate buffer table: %v", err)
	}

	t.logger.Debugf("Table buffer flush: finished")

	return nil
}

func (t *genericTable) saveLSN() error {
	t.logger.Debugf("lsn %s saved", t.txFinalLSN)

	return t.persStorage.WriteStream(
		t.cfg.PgTableName.KeyName(),
		bytes.NewReader(t.txFinalLSN.FormattedBytes()),
		true)
}

func (t *genericTable) flush() error {
	t.logger.Debugf("flush (in sync: %t, txFinalLSN: %v)", t.inSync, t.txFinalLSN)
	if err := t.flushMemBuffer(); err != nil {
		return fmt.Errorf("could not flush buffers: %v", err)
	}

	if t.cfg.ChBufferTable.IsEmpty() || t.memBufferFlushCnt == 0 {
		t.logger.Debugf("Flush to main table: nothing to flush")
		if !t.inSync {
			if err := t.saveLSN(); err != nil {
				return fmt.Errorf("could not save lsn to file: %v", err)
			}
		}
		return nil
	}

	defer func(startTime time.Time, rows uint64) {
		t.logger.Infof("Flush to main table: processed in %v (rows: %d)",
			time.Since(startTime).Truncate(time.Second), rows)
	}(time.Now(), t.memBufferRowId)

	if err := utils.Try(t.ctx, maxAttempts, attemptInterval, t.attemptFlushTblBuffer); err != nil {
		return err
	}

	if !t.inSync {
		if err := t.saveLSN(); err != nil {
			return fmt.Errorf("could not save lsn to file: %v", err)
		}
	}

	return nil
}

//FlushToMainTable flushes data from buffer table to the main one
func (t *genericTable) FlushToMainTable() error {
	t.logger.Debugf("Flush to main table: trying to acquire table lock")
	t.Lock()
	t.logger.Debugf("Flush to main table: table lock acquired")
	defer t.Unlock()

	return t.flush()
}

func (t *genericTable) convertRow(row message.Row) chTuple {
	var buf bytes.Buffer
	bufSize := 0
	for _, v := range row {
		bufSize += len(v.Value) + 1 // and delimiter
	}
	if t.cfg.GenerationColumn != "" {
		bufSize += 9 // for generationID + delimiter
	}
	buf.Grow(bufSize)

	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if colId > 0 {
			buf.WriteByte(columnDelimiter)
		}
		chutils.ConvertColumn(&buf, t.cfg.PgColumns[col.Name], row[colId], t.cfg.ColumnProperties[col.Name])
	}

	if t.cfg.GenerationColumn != "" {
		if buf.Len() > 0 {
			buf.WriteByte(columnDelimiter)
		}
		buf.WriteString(strconv.FormatUint(*t.generationID, 10))
	}

	return buf.Bytes()
}

// Truncate truncates main and buffer(if used) tables
func (t *genericTable) Truncate() error {
	t.memBufferPgDMLsCnt = 0

	if err := t.truncateTable(t.cfg.ChMainTable); err != nil {
		return err
	}

	if !t.cfg.ChBufferTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChBufferTable); err != nil {
			return err
		}
	}

	return nil
}

// Start performs initialization
func (t *genericTable) Init() error {
	if !t.cfg.ChBufferTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChBufferTable); err != nil {
			return err
		}
	}

	if !t.cfg.ChSyncAuxTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChSyncAuxTable); err != nil {
			return err
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
		}
	}

	return equal, keyColumnChanged
}

func appendField(str []byte, fields ...[]byte) []byte {
	if len(str) == 0 {
		finalLen := 0
		for _, v := range fields {
			finalLen += len(v)
		}
		if len(fields) > 0 {
			finalLen += len(fields) - 1
		}
		res := make([]byte, finalLen)

		p := 0
		for i, v := range fields {
			if i > 0 {
				res[p-1] = '\t'
			}
			copy(res[p:], v)
			p += 1 + len(v)
		}

		return res
	} else {
		finalLen := len(str)
		for _, v := range fields {
			finalLen += 1 + len(v)
		}
		res := make([]byte, finalLen)
		copy(res, str)

		p := len(str) + 1
		for _, v := range fields {
			res[p-1] = '\t'
			copy(res[p:], v)
			p += 1 + len(v)
		}

		return res
	}
}

func (t *genericTable) bufTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn)
}

func (t *genericTable) Begin(finalLSN dbtypes.LSN) error {
	t.logger.Debugf("Begin: trying to acquire table lock")
	t.Lock()
	t.logger.Debugf("Begin: table lock acquired")
	t.txFinalLSN = finalLSN

	return nil
}

func (t *genericTable) Commit(flush bool) error {
	defer t.Unlock()
	t.logger.Debugf("Commit: with %t flush", flush)
	if !flush {
		t.logger.Debugf("Commit: finished")
		return nil
	}

	if err := t.flush(); err != nil {
		return fmt.Errorf("could not flush: %v", err)
	}

	t.logger.Debugf("Begin: finished")
	return nil
}

func (t *genericTable) String() string {
	return t.cfg.PgTableName.String()
}

func (ch chTuple) String() string {
	return string(ch)
}
