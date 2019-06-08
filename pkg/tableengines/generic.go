package tableengines

import (
	"bytes"
	"compress/gzip"
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
	"github.com/mkabilov/pg2ch/pkg/utils/chloader"
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
	ctx      context.Context
	chLoader *chloader.CHLoader

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

	syncLastBatchTime time.Time //to calculate speed
	syncGzWriter      *gzip.Writer
}

func newGenericTable(ctx context.Context, connUrl, dbName string, tblCfg config.Table, genID *uint64) genericTable {
	t := genericTable{
		ctx:           ctx,
		chLoader:      chloader.New(connUrl, dbName),
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		flushMutex:    &sync.Mutex{},
		tupleColumns:  tblCfg.TupleColumns,
		generationID:  genID,
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

func (t *genericTable) pgStatLiveTuples(pgTx *pgx.Tx) (int64, error) {
	var rows sql.NullInt64
	err := pgTx.QueryRow("select n_live_tup from pg_stat_all_tables where schemaname = $1 and relname = $2",
		t.cfg.PgTableName.SchemaName,
		t.cfg.PgTableName.TableName).Scan(&rows)
	if err != nil || !rows.Valid {
		return 0, err
	}

	return rows.Int64, nil
}

func convertColumn(colType string, val message.Tuple, colProps config.ColumnProperty) []byte {
	switch colType {
	case utils.PgIstore:
		fallthrough
	case utils.PgBigIstore:
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
	case utils.PgAjBool:
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

	if _, err := t.syncGzWriter.Write(t.convertRow(row)); err != nil {
		return err
	}

	t.bufferCmdId++

	return nil
}

func (t *genericTable) syncFlushGzip() {
	if t.bufferCmdId%syncProgressBatch == 0 {
		var destinationTable string
		if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
			destinationTable = t.cfg.ChBufferTable
		} else {
			destinationTable = t.cfg.ChMainTable
		}

		log.Printf("copied %d from %q to %q (speed: %.0f rows/s)",
			t.bufferCmdId, t.cfg.PgTableName.String(), destinationTable, float64(syncProgressBatch)/time.Since(t.syncLastBatchTime).Seconds())

		t.syncLastBatchTime = time.Now()
	}

	if t.bufferCmdId%gzipFlushCount == 0 {
		if err := t.syncGzWriter.Flush(); err != nil {
			log.Printf("could not gzip flush: %v", err)
		}
	}
}

func (t *genericTable) genSync(pgTx *pgx.Tx, w io.Writer) error {
	var destinationTable string

	tblLiveTuples, err := t.pgStatLiveTuples(pgTx)
	if err != nil {
		log.Printf("Could not get approx number of rows in the source table: %v", err)
	}

	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		log.Printf("Copy from %q postgres table to %q clickhouse table via %q buffer table started. ~%v rows to copy",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable, t.cfg.ChBufferTable, tblLiveTuples)
		if err := t.truncateBufTable(); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
		destinationTable = t.cfg.ChBufferTable
	} else {
		log.Printf("Copy from %q postgres table to %q clickhouse table started. ~%v rows to copy",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable, tblLiveTuples)
		if !t.cfg.InitSyncSkipTruncate {
			if err := t.truncateMainTable(); err != nil {
				return fmt.Errorf("could not truncate main table: %v", err)
			}
		}
		destinationTable = t.cfg.ChMainTable
	}

	t.syncGzWriter, err = gzip.NewWriterLevel(t.chLoader, gzip.BestSpeed)
	if err != nil {
		return fmt.Errorf("could not initialize gzip: %v", err)
	}

	t.bufferCmdId = 0

	loaderErrCh := make(chan error, 1)
	go func(errCh chan error) {
		errCh <- t.chLoader.BulkUpload(destinationTable, t.chUsedColumns)
	}(loaderErrCh)

	query := fmt.Sprintf("copy %s(%s) to stdout", t.cfg.PgTableName.String(), strings.Join(t.pgUsedColumns, ", "))
	log.Printf("copy command: %q", query)

	t.syncLastBatchTime = time.Now()
	ct, err := pgTx.CopyToWriter(w, query)
	if err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.syncGzWriter.Flush(); err != nil {
		return fmt.Errorf("could not gzip flush: %v", err)
	}

	if err := t.syncGzWriter.Close(); err != nil {
		return fmt.Errorf("could not close gzip: %v", err)
	}

	if err := t.chLoader.PipeFinishWriting(); err != nil {
		return fmt.Errorf("could not close pipe: %v", err)
	}

	log.Printf("copy: %d rows", ct.RowsAffected())
	log.Printf("inserted to CH: %d rows", t.bufferCmdId)

	t.bufferCmdId = 0

	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)

	return nil
}

func (t *genericTable) processChTuples(set chTuples) (bool, error) {
	if set != nil {
		for _, row := range set {
			t.chLoader.WriteLine(row)
			t.bufferRowId++
		}
		t.bufferCmdId++
	}

	if t.bufferCmdId == t.cfg.MaxBufferLength {
		t.flushMutex.Lock()
		defer t.flushMutex.Unlock()

		if err := t.flushBuffer(); err != nil {
			return false, fmt.Errorf("could not flush buffer: %v", err)
		}
	}

	if t.cfg.ChBufferTable == "" {
		return false, nil
	}

	return t.bufferFlushCnt >= t.cfg.FlushThreshold, nil
}

func (t *genericTable) flushBuffer() error {
	var err error

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

	if err := t.chLoader.Upload(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
		return err
	}
	log.Printf("buffer flushed %d commands", t.bufferCmdId)

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
	t.flushMutex.Lock()
	defer t.flushMutex.Unlock()

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
func (t *genericTable) Truncate() error {
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
