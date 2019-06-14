package tableengines

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/loader"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

// Generic table is a "parent" struct for all the table engines
const (
	attemptInterval = time.Second
	maxAttempts     = 100

	pgTrue  = 't'
	pgFalse = 'f'

	//ajBool specific value
	ajBoolUnknown   = 'u'
	columnDelimiter = '\t'

	timestampLength = 19 // ->2019-06-08 15:50:01<- clickhouse does not support milliseconds

	syncProgressBatch = 1000000
	gzipFlushCount    = 1000
)

var (
	zeroStr            = []byte("0")
	oneStr             = []byte("1")
	minusOneStr        = []byte("-1")
	ajBoolUnkownValue  = []byte("2")
	nullStr            = []byte(`\N`)
	columnDelimiterStr = []byte("\t")

	istoreNull = []byte("[]\t[]")
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

	syncLastBatchTime time.Time //to calculate rate

	inSync        bool
	inSyncRowID   uint64
	syncRows      uint64
	syncTotalRows uint64

	bulkuploader *bulkupload.BulkUpload
	minLSN       utils.LSN
}

func newGenericTable(ctx context.Context, connUrl, dbName string, tblCfg config.Table, genID *uint64) genericTable {
	t := genericTable{
		Mutex:         sync.Mutex{},
		ctx:           ctx,
		chLoader:      loader.New(connUrl, dbName),
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		flushMutex:    &sync.Mutex{},
		tupleColumns:  tblCfg.TupleColumns,
		generationID:  genID,
		bulkuploader:  bulkupload.New(connUrl, dbName, gzipFlushCount),
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

func (t *genericTable) writeLSN(lsn utils.LSN) error {
	if err := t.chLoader.BufferWrite(append(lsn.StrBytes(), '\t')); err != nil {
		return err
	}

	return nil
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

func (t *genericTable) truncateMainTable() error {
	if err := t.chLoader.Exec(fmt.Sprintf("truncate table %s", t.cfg.ChMainTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateBufTable() error {
	if t.cfg.ChBufferTable == "" {
		return nil
	}

	if err := t.chLoader.Exec(fmt.Sprintf("truncate table %s", t.cfg.ChBufferTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) pgStatLiveTuples(pgTx *pgx.Tx) (uint64, error) {
	var rows sql.NullInt64
	err := pgTx.QueryRow("select n_live_tup from pg_stat_all_tables where schemaname = $1 and relname = $2",
		t.cfg.PgTableName.SchemaName,
		t.cfg.PgTableName.TableName).Scan(&rows)
	if err != nil || !rows.Valid {
		return 0, err
	}

	return uint64(rows.Int64), nil
}

func convertColumn(colType string, val message.Tuple, colProps config.ColumnProperty) []byte {
	switch colType {
	case utils.PgAdjustIstore:
		fallthrough
	case utils.PgAdjustBigIstore:
		if colProps.FlattenIstore {
			if val.Kind == message.TupleNull {
				return []byte(strings.Repeat("\t\\N", colProps.FlattenIstoreMax-colProps.FlattenIstoreMin+1))[1:]
			}

			return utils.IstoreValues(val.Value, colProps.FlattenIstoreMin, colProps.FlattenIstoreMax)
		} else {
			if val.Kind == message.TupleNull {
				return istoreNull
			}

			return utils.IstoreToArrays(val.Value)
		}
	case utils.PgAdjustAjBool:
		fallthrough
	case utils.PgBoolean:
		if val.Kind == message.TupleNull {
			return nullStr
		}

		switch val.Value[0] {
		case pgTrue:
			return oneStr
		case pgFalse:
			return zeroStr
		case ajBoolUnknown:
			return ajBoolUnkownValue
		}
	case utils.PgTimestampWithTimeZone:
		fallthrough
	case utils.PgTimestamp:
		if val.Kind == message.TupleNull {
			return nullStr
		}

		return val.Value[:timestampLength]

	case utils.PgTime:
		fallthrough
	case utils.PgTimeWithoutTimeZone:
		//TODO
	case utils.PgTimeWithTimeZone:
		//TODO
	}
	if val.Kind == message.TupleNull {
		return nullStr
	}

	return val.Value
}

func (t *genericTable) genSyncWrite(p []byte) error {
	row, err := pgutils.DecodeCopyToTuples(p)
	if err != nil {
		return fmt.Errorf("could not parse copy string: %v", err)
	}

	if err := t.bulkuploader.Write(t.convertRow(row)); err != nil {
		return err
	}

	t.syncRows++

	return nil
}

func (t *genericTable) genSync(pgTx *pgx.Tx, lsn utils.LSN, w io.Writer) error {
	t.minLSN = lsn
	tblLiveTuples, err := t.pgStatLiveTuples(pgTx)
	if err != nil {
		log.Printf("Could not get approx number of rows in the source table: %v", err)
	}
	t.syncTotalRows = tblLiveTuples

	log.Printf("Copy from %q postgres table to %q clickhouse table started. ~%d rows to copy",
		t.cfg.PgTableName.String(), t.cfg.ChMainTable, t.syncTotalRows)
	if !t.cfg.InitSyncSkipTruncate {
		if err := t.truncateMainTable(); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}
	}
	t.syncRows = 0

	loaderErrCh := make(chan error, 1)
	go func(errCh chan error) {
		if err := t.bulkuploader.BulkUpload(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
			errCh <- err
			log.Fatalf("could not sync upload: %v", err)
		}
		errCh <- nil
	}(loaderErrCh)

	t.syncLastBatchTime = time.Now()
	ct, err := pgTx.CopyToWriter(w, fmt.Sprintf(
		"copy %s(%s) to stdout",
		t.cfg.PgTableName.String(), strings.Join(t.pgUsedColumns, ", ")))
	if err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.bulkuploader.PipeFinishWriting(); err != nil {
		return fmt.Errorf("could not close gzip: %v", err)
	}

	if ct.RowsAffected() != int64(t.syncRows) {
		return fmt.Errorf("number of rows inserted to clickhouse doesn't match the initial number of rows in pg")
	}

	log.Printf("%s: copied during sync: %d rows", t.cfg.PgTableName, t.syncRows)

	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)

	return t.postSync(lsn)
}

func (t *genericTable) postSync(lsn utils.LSN) error {
	// post sync
	log.Printf("%s: starting post sync. waiting for current tx to finish", t.cfg.PgTableName)
	t.Lock()
	defer t.Unlock()

	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffer: %v", err)
	}

	chColumns := strings.Join(t.chUsedColumns, ",")

	log.Printf("%s: delta size: %s", t.cfg.PgTableName, t.deltaSize(lsn))

	query := fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s WHERE %s > %d ORDER BY %s",
		t.cfg.ChMainTable, chColumns, chColumns, t.cfg.ChSyncAuxTable, t.cfg.LsnColumnName, uint64(lsn), t.cfg.BufferTableRowIdColumn)

	if err := t.chLoader.Exec(query); err != nil {
		return fmt.Errorf("could not merge with sync aux table: %v", err)
	}

	if err := t.chLoader.Exec(fmt.Sprintf("TRUNCATE TABLE %s", t.cfg.ChSyncAuxTable)); err != nil {
		return fmt.Errorf("could not truncat table: %v", err)
	}

	t.inSync = false

	return nil
}

func (t *genericTable) processChTuples(lsn utils.LSN, set chTuples) (mergeIsNeeded bool, err error) {
	if set != nil {
		for _, row := range set {
			if t.inSync {
				row = append(row, '\t')
				row = append(row, strconv.FormatUint(t.inSyncRowID, 10)...)
				row = append(row, '\t')
				row = append(row, lsn.StrBytes()...)

				t.inSyncRowID++
			} else {
				if lsn < t.minLSN {
					log.Printf("skipping tuples: %v < %v", uint64(lsn), uint64(t.minLSN))
					return false, nil
				}
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

	if t.cfg.ChBufferTable == "" {
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

func (t *genericTable) printSyncProgress() {
	if t.syncRows%syncProgressBatch == 0 {
		var eta time.Duration
		speed := float64(syncProgressBatch) / time.Since(t.syncLastBatchTime).Seconds()
		if t.syncRows < t.syncTotalRows {
			eta = time.Second * time.Duration((t.syncTotalRows-t.syncRows)/uint64(speed))
		}

		log.Printf("%s: %d rows copied to %q (ETA: %v speed: %.0f rows/s)",
			t.cfg.PgTableName.String(), t.syncRows, t.cfg.ChMainTable, eta.Truncate(time.Second), speed)

		t.syncLastBatchTime = time.Now()
	}
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
			return fmt.Errorf("could not flush buffer for %q table: %v", t.cfg.ChMainTable)
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
func (t *genericTable) FlushToMainTable() error {
	t.Lock()
	defer t.Unlock()

	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffers: %v", err)
	}

	if t.cfg.ChBufferTable == "" || t.bufferFlushCnt == 0 {
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

	if err := t.truncateBufTable(); err != nil {
		return fmt.Errorf("could not truncate buffer table: %v", err)
	}

	return nil
}

func (t *genericTable) convertRow(row message.Row) chTuple {
	res := make([]byte, 0)

	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		values := convertColumn(t.cfg.PgColumns[col.Name].BaseType, row[colId], t.cfg.ColumnProperties[col.Name])
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

	if err := t.truncateMainTable(); err != nil {
		return err
	}

	return t.truncateBufTable()
}

// Init performs initialization
func (t *genericTable) Init() error {
	return t.truncateBufTable()
}

// SetTupleColumns sets the tuple columns
func (t *genericTable) SetTupleColumns(tupleColumns []message.Column) {
	//TODO: suggest alter table message for adding/deleting new/old columns on clickhouse side
	t.tupleColumns = tupleColumns
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

func (t *genericTable) syncAuxTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn, t.cfg.LsnColumnName)
}

func (t *genericTable) bufTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn)
}

func (t *genericTable) InitSync() error {
	if err := t.chLoader.Exec(fmt.Sprintf("TRUNCATE TABLE %s", t.cfg.ChSyncAuxTable)); err != nil {
		return fmt.Errorf("could not truncat table: %v", err)
	}
	t.inSync = true

	return nil
}

func (t *genericTable) deltaSize(lsn utils.LSN) string {
	res, err := t.chLoader.Query(fmt.Sprintf("SELECT count() FROM %s WHERE %s > %d",
		t.cfg.ChSyncAuxTable, t.cfg.LsnColumnName, uint64(lsn)))
	if err != nil {
		log.Fatalf("query error: %v", err)
	}
	return res[0][0]
}

func (t *genericTable) Begin() error {
	t.Lock()
	return nil
}

func (t *genericTable) Commit() error {
	t.Unlock()
	return nil
}
