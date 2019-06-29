package tableengines

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/peterbourgon/diskv"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/loader"
)

// Generic table is a "parent" struct for all the table engines
const (
	attemptInterval = time.Second
	maxAttempts     = 100

	columnDelimiter = '\t'
)

var (
	zeroStr            = []byte("0")
	oneStr             = []byte("1")
	minusOneStr        = []byte("-1")
	columnDelimiterStr = []byte("\t")
)

type chTuple []byte
type chTuples []chTuple

type genericTable struct {
	sync.Mutex

	ctx      context.Context
	chLoader *loader.CHLoader

	cfg config.Table

	chUsedColumns  []string
	pgUsedColumns  []string
	columnMapping  map[string]config.ChColumn // [pg column name]ch column description
	flushMutex     *sync.Mutex
	bufferCmdId    int // number of pg DML commands in the current buffer, i.e. 1 update pg dml command => 2 ch inserts
	bufferRowId    int // row id in the buffer, will be used as a sorting column while flushing to the main table
	bufferFlushCnt int // number of flushed buffers
	flushQueries   []string
	tupleColumns   []message.Column // Columns description taken from RELATION rep message
	generationID   *uint64

	inSync            bool // protected via table mutex
	syncedRows        uint64
	rowsToSync        uint64
	syncLastBatchTime time.Time //to calculate rate
	auxTblRowID       uint64

	bulkUploader    *bulkupload.BulkUpload
	syncSnapshotLSN utils.LSN // LSN of the initial copy snapshot
	persStorage     *diskv.Diskv
}

func newGenericTable(ctx context.Context, persStorage *diskv.Diskv, connUrl string, tblCfg config.Table, genID *uint64) genericTable {
	t := genericTable{
		Mutex:         sync.Mutex{},
		ctx:           ctx,
		chLoader:      loader.New(connUrl),
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		flushMutex:    &sync.Mutex{},
		tupleColumns:  tblCfg.TupleColumns,
		generationID:  genID,
		bulkUploader:  bulkupload.New(connUrl, gzipFlushCount),
		persStorage:   persStorage,
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
				if columnCfg.FlattenIstore {
					for i := columnCfg.FlattenIstoreMin; i <= columnCfg.FlattenIstoreMax; i++ {
						t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%d", chCol.Name, i))
					}
				} else {
					t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreKeysSuffix))
					t.chUsedColumns = append(t.chUsedColumns, fmt.Sprintf("%s_%s", chCol.Name, columnCfg.IstoreValuesSuffix))
				}
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

func (t *genericTable) writeLine(val []byte) error {
	if err := t.chLoader.BufferWrite(val); err != nil {
		return err
	}

	if err := t.chLoader.BufferWrite([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateTable(tableName config.ChTableName) error {
	if err := t.chLoader.Exec(fmt.Sprintf("truncate table %s.%s", tableName.DatabaseName, tableName.TableName)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) processChTuples(lsn utils.LSN, set chTuples) (mergeIsNeeded bool, err error) {
	if set != nil {
		for _, row := range set {
			if t.inSync {
				row = append(row, '\t')
				row = append(row, strconv.FormatUint(t.auxTblRowID, 10)...)
				row = append(row, '\t')
				row = append(row, lsn.StrBytes()...)

				t.auxTblRowID++
			} else if lsn < t.syncSnapshotLSN {
				return false, nil
			}

			if err := t.writeLine(row); err != nil {
				return false, err
			}

			t.bufferRowId++
		}

		t.bufferCmdId++
	}

	if t.bufferCmdId == t.cfg.MaxBufferLength {
		if err := t.flushBuffer(); err != nil {
			return false, err
		}
	}

	if t.cfg.ChBufferTable.IsEmpty() {
		return false, nil
	}

	return t.bufferFlushCnt >= t.cfg.FlushThreshold, nil
}

func (t *genericTable) flushBuffer() error {
	var err error
	t.flushMutex.Lock()
	defer t.flushMutex.Unlock()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = t.attemptFlushBuffer()
		if err == nil {
			if attempt > 0 {
				log.Printf("succeeded buffer flush after %v attempts", attempt)
			}
			break
		}

		log.Printf("could not flush buffer: %v, retrying after %v", err, attemptInterval)
		select {
		case <-t.ctx.Done():
			return fmt.Errorf("abort retrying")
		case <-time.After(attemptInterval):
		}
	}

	return err
}

// flush from memory to the buffer/main table
func (t *genericTable) attemptFlushBuffer() error {
	if t.bufferCmdId == 0 {
		return nil
	}

	if t.inSync {
		if err := t.chLoader.BufferFlush(t.cfg.ChSyncAuxTable, t.syncAuxTableColumns()); err != nil {
			return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChSyncAuxTable, err)
		}
	} else {
		if err := t.chLoader.BufferFlush(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
			return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChMainTable, err)
		}
	}

	t.bufferCmdId = 0
	t.bufferFlushCnt++

	return nil
}

func (t *genericTable) tryFlushToMainTable() error { //TODO: consider better name
	for _, query := range t.flushQueries {
		if err := t.chLoader.Exec(query); err != nil {
			return err
		}
	}

	t.bufferFlushCnt = 0
	t.bufferRowId = 0

	return nil
}

//FlushToMainTable flushes data from buffer table to the main one
func (t *genericTable) FlushToMainTable(lsn utils.LSN) error {
	t.Lock()
	defer t.Unlock()

	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffers: %v", err)
	}

	if t.cfg.ChBufferTable.IsEmpty() || t.bufferFlushCnt == 0 {
		return nil
	}

	defer func(startTime time.Time, rows int) {
		log.Printf("FlushToMainTable for %s pg table processed in %v (rows: %d)",
			t.cfg.PgTableName.String(), time.Since(startTime).Truncate(time.Second), rows)
	}(time.Now(), t.bufferRowId)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := t.tryFlushToMainTable()
		if err == nil {
			if attempt > 0 {
				log.Printf("succeeded flush to main table after %v attempts", attempt)
			}
			break
		}

		log.Printf("could not flush: %v, retrying after %v", err, attemptInterval)
		select {
		case <-t.ctx.Done():
			return fmt.Errorf("abort retrying")
		case <-time.After(attemptInterval):
		}
	}

	if !t.cfg.ChBufferTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChBufferTable); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
	}

	if !t.inSync {
		if err := t.persStorage.Write(t.cfg.PgTableName.KeyName(), lsn.FormattedBytes()); err != nil {
			return fmt.Errorf("could not save lsn for table %q: %v", t.cfg.PgTableName, err)
		}
	}

	return nil
}

func (t *genericTable) convertRow(row message.Row) chTuple {
	res := make([]byte, 0)

	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		values := chutils.ConvertColumn(t.cfg.PgColumns[col.Name].BaseType, row[colId], t.cfg.ColumnProperties[col.Name])
		if colId > 0 {
			res = append(res, columnDelimiter)
		}
		res = append(res, values...)
	}

	if t.cfg.GenerationColumn != "" {
		if len(res) > 0 {
			res = append(res, columnDelimiter)
		}
		res = append(res, []byte(strconv.FormatUint(*t.generationID, 10))...)
	}

	return res
}

// Truncate truncates main and buffer(if used) tables
func (t *genericTable) Truncate(lsn utils.LSN) error {
	t.bufferCmdId = 0

	if err := t.truncateTable(t.cfg.ChMainTable); err != nil {
		return err
	}

	if t.cfg.ChBufferTable.IsEmpty() {
		return nil
	}

	return t.truncateTable(t.cfg.ChBufferTable)
}

// Start performs initialization
func (t *genericTable) Init() error {
	if t.cfg.ChBufferTable.IsEmpty() {
		return nil
	}

	return t.truncateTable(t.cfg.ChBufferTable)
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
		res := make([]byte, 0)
		for _, v := range fields {
			res = append(res, v...)
		}

		return res
	} else {
		res := make([]byte, len(str))
		copy(res, str)
		for _, v := range fields {
			res = append(res, append(columnDelimiterStr, v...)...)
		}

		return res
	}
}

func (t *genericTable) bufTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn)
}

func (t *genericTable) Begin() error {
	t.Lock()
	return nil
}

func (t *genericTable) Commit() error {
	t.Unlock()
	return nil
}
