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

	"github.com/mkabilov/pg2ch/pkg/utils/chloader"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

// Generic table is a "parent" struct for all the table engines
const (
	attemptInterval = time.Second
	maxAttempts     = 100

	pgTrue  = "t"
	pgFalse = "f"
)

var (
	zeroStr     = sql.NullString{String: "0", Valid: true}
	oneStr      = sql.NullString{String: "1", Valid: true}
	minusOneStr = sql.NullString{String: "-1", Valid: true}
)

type bufRow struct {
	rowID int
	data  []sql.NullString
}

type commandSet [][]sql.NullString

type bufCommand []bufRow

type genericTable struct {
	ctx      context.Context
	chLoader *chloader.CHLoader

	cfg config.Table

	chUsedColumns  []string
	pgUsedColumns  []string
	columnMapping  map[string]config.ChColumn // [pg column name]ch column description
	flushMutex     *sync.Mutex
	buffer         []bufCommand
	bufferCmdId    int // number of commands in the current buffer
	bufferRowId    int // row id in the buffer
	bufferFlushCnt int // number of flushed buffers
	flushQueries   []string
	tupleColumns   []message.Column // Columns description taken from RELATION rep message
	generationID   *uint64
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

	t.buffer = make([]bufCommand, t.cfg.MaxBufferLength)

	for _, pgCol := range t.tupleColumns {
		chCol, ok := tblCfg.ColumnMapping[pgCol.Name]
		if !ok {
			continue
		}

		t.columnMapping[pgCol.Name] = chCol
		t.pgUsedColumns = append(t.pgUsedColumns, pgCol.Name)

		if pgCol.TypeOID == utils.IstoreOID || pgCol.TypeOID == utils.BigIstoreOID {
			t.chUsedColumns = append(t.chUsedColumns, pgCol.Name+"_keys")
			t.chUsedColumns = append(t.chUsedColumns, pgCol.Name+"_values")
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

func convertColumn(colOID utils.OID, value string) ([]string, error) {
	switch colOID {
	case utils.IstoreOID:
		fallthrough
	case utils.BigIstoreOID:
		keys, values, err := utils.SplitIstore(value)
		if err != nil {
			return nil, fmt.Errorf("could not parse istore: %v", err)
		}

		return []string{"[" + strings.Join(keys, ",") + "]", "[" + strings.Join(values, ",") + "]"}, nil
	case utils.AjBoolOID:
		fallthrough
	case utils.BoolOID:
		if value == "t" {
			return []string{"1"}, nil
		} else if value == "f" {
			return []string{"0"}, nil
		} else if value == "u" {
			return []string{"2"}, nil
		}
	}

	return []string{value}, nil
}

func (t *genericTable) genWrite(p []byte) error {
	fields := strings.Split(string(p), "\t") //TODO: rewrite split function into single scan of a string

	if len(fields) != len(t.tupleColumns) {
		return fmt.Errorf("fields number mismatch")
	}

	for pgId, pgCol := range t.tupleColumns {
		if pgId != 0 {
			if err := t.chLoader.BulkAdd([]byte("\t")); err != nil {
				return err
			}
		}

		if fields[pgId] == `\N` {
			if err := t.chLoader.BulkAdd([]byte(fields[pgId])); err != nil {
				return err
			}
		}

		val, err := convertColumn(pgCol.TypeOID, fields[pgId])
		if err != nil {
			return err
		}

		if err := t.chLoader.BulkAdd([]byte(strings.Join(val, "\t"))); err != nil {
			return err
		}

		switch pgCol.TypeOID {
		case utils.IstoreOID:
			fallthrough
		case utils.BigIstoreOID:
			keys, values, err := utils.SplitIstore(fields[pgId])
			if err != nil {
				return fmt.Errorf("could not parse istore: %v", err)
			}

			if err := t.chLoader.BulkAdd([]byte("[" + strings.Join(keys, ",") + "]\t")); err != nil {
				return err
			}

			if err := t.chLoader.BulkAdd([]byte("[" + strings.Join(values, ",") + "]")); err != nil {
				return err
			}
		case utils.AjBoolOID:
			fallthrough
		case utils.BoolOID:
			if fields[pgId] == "t" {
				if err := t.chLoader.BulkAdd([]byte("1")); err != nil {
					return err
				}
			} else if fields[pgId] == "f" {
				if err := t.chLoader.BulkAdd([]byte("0")); err != nil {
					return err
				}

			} else if fields[pgId] == "u" {
				if err := t.chLoader.BulkAdd([]byte("2")); err != nil {
					return err
				}
			}
		default:
			if err := t.chLoader.BulkAdd([]byte(fields[pgId])); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *genericTable) genSync(pgTx *pgx.Tx, w io.Writer) error {
	if t.cfg.InitSyncSkip {
		return nil
	}

	tblLiveTuples, err := t.pgStatLiveTuples(pgTx)
	if err != nil {
		log.Printf("Could not get approx number of rows in the source table: %v", err)
	}

	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		log.Printf("Copy from %s postgres table to %q clickhouse table via %q buffer table started. ~%v rows to copy",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable, t.cfg.ChBufferTable, tblLiveTuples)
		if err := t.truncateBufTable(); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
	} else {
		log.Printf("Copy from %s postgres table to %q clickhouse table started. ~%v rows to copy",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable, tblLiveTuples)
		if !t.cfg.InitSyncSkipTruncate {
			if err := t.truncateMainTable(); err != nil {
				return fmt.Errorf("could not truncate main table: %v", err)
			}
		}
	}

	loaderErrCh := make(chan error, 1)
	go func(errCh chan error) {
		errCh <- t.chLoader.BulkUpload(t.cfg.ChMainTable, nil)
	}(loaderErrCh)

	query := fmt.Sprintf("copy %s(%s) to stdout", t.cfg.PgTableName.String(), strings.Join(t.pgUsedColumns, ", "))
	if _, err := pgTx.CopyToWriter(w, query); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	t.chLoader.BulkFinish()
	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)

	rows := t.bufferRowId
	t.bufferCmdId = 0
	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		if !t.cfg.InitSyncSkipTruncate {
			if err := t.truncateMainTable(); err != nil {
				return fmt.Errorf("could not truncate main table: %v", err)
			}
		}

		t.bufferFlushCnt++
		if err := t.FlushToMainTable(); err != nil {
			return fmt.Errorf("could not move from buffer to the main table: %v", err)
		}
	}
	t.bufferRowId = 0
	log.Printf("Pg table %s: %d rows copied to ClickHouse %q table", t.cfg.PgTableName.String(), rows, t.cfg.ChMainTable)

	return nil
}

func (t *genericTable) bufferAppend(cmdSet commandSet) {
	bufItem := make([]bufRow, len(cmdSet))
	for i := range cmdSet {
		bufItem[i] = bufRow{rowID: t.bufferRowId, data: cmdSet[i]}
		t.bufferRowId++
	}

	t.buffer[t.bufferCmdId] = bufItem
	t.bufferCmdId++
}

func (t *genericTable) writeLine(vals []sql.NullString) {
	ln := len(vals) - 1
	if ln == -1 {
		return
	}
	for colID, col := range vals {
		if col.Valid {
			col, err := convertColumn(t.tupleColumns[colID].TypeOID, col.String)
			log.Printf("col: %v", col)
			if err != nil {
				panic(err)
			}
			t.chLoader.Write([]byte(strings.Join(col, "\t")))
		} else {
			t.chLoader.Write([]byte(`\N`))
		}

		if colID != ln {
			t.chLoader.Write([]byte("\t"))
		}
	}

	t.chLoader.Write([]byte("\n"))
}

func (t *genericTable) processCommandSet(set commandSet) (bool, error) {
	if set != nil {
		for _, row := range set {
			t.chLoader.Add(row)
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

func (t *genericTable) syncConvertIntoRow(p []byte) ([]interface{}, int, error) {
	rec, err := utils.DecodeCopy(p)
	if err != nil {
		return nil, 0, err
	}

	row, err := t.syncConvertStrings(rec)
	if err != nil {
		return nil, 0, fmt.Errorf("could not parse record: %v", err)
	}

	return row, len(p), nil
}

func (t *genericTable) insertRow(row []sql.NullString) error {
	var chTableName string

	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		chTableName = t.cfg.ChBufferTable
		row = append(row, sql.NullString{String: strconv.Itoa(t.bufferRowId), Valid: true})
	} else {
		chTableName = t.cfg.ChMainTable
	}
	t.chLoader.Add(row)
	if err := t.chLoader.Upload(t.cfg.ChMainTable, t.chUsedColumns); err != nil {
		return err
	}

	t.bufferRowId++

	if t.bufferRowId%1000000 == 0 {
		log.Printf("clickhouse %q table: %d rows inserted", chTableName, t.bufferRowId)
	}

	return nil
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

func convert(val string, chType config.ChColumn, pgType config.PgColumn) (interface{}, error) {
	switch chType.BaseType {
	case utils.ChInt8:
		return strconv.ParseInt(val, 10, 8)
	case utils.ChInt16:
		return strconv.ParseInt(val, 10, 16)
	case utils.ChInt32:
		return strconv.ParseInt(val, 10, 32)
	case utils.ChInt64:
		return strconv.ParseInt(val, 10, 64)
	case utils.ChUInt8:
		if pgType.BaseType == utils.PgBoolean {
			if val == pgTrue {
				return 1, nil
			} else if val == pgFalse {
				return 0, nil
			}
		}

		return nil, fmt.Errorf("can't convert %v to %v", pgType.BaseType, chType.BaseType)
	case utils.ChUInt16:
		return strconv.ParseUint(val, 10, 16)
	case utils.ChUint32:
		if pgType.BaseType == utils.PgTimeWithoutTimeZone {
			t, err := time.Parse("15:04:05", val)
			if err != nil {
				return nil, err
			}

			return t.Hour()*3600 + t.Minute()*60 + t.Second(), nil
		}

		return strconv.ParseUint(val, 10, 32)
	case utils.ChUint64:
		return strconv.ParseUint(val, 10, 64)
	case utils.ChFloat32:
		return strconv.ParseFloat(val, 32)
	case utils.ChDecimal:
		return strconv.ParseFloat(val, 64)
	case utils.ChFloat64:
		return strconv.ParseFloat(val, 64)
	case utils.ChFixedString:
		fallthrough
	case utils.ChString:
		return val, nil
	case utils.ChDate:
		return time.Parse("2006-01-02", val[:10])
	case utils.ChDateTime:
		return time.Parse("2006-01-02 15:04:05", val[:19])
	}

	return nil, fmt.Errorf("unknown type: %v", chType)
}

func (t *genericTable) convertTuples(row message.Row) []sql.NullString {
	res := make([]sql.NullString, 0)

	for colId, col := range t.tupleColumns {
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if row[colId].Kind == message.TupleNull {
			res = append(res, sql.NullString{Valid: false})
		}

		values, err := convertColumn(col.TypeOID, string(row[colId].Value))
		if err != nil {
			panic(err)
		}
		for _, val := range values {
			res = append(res, sql.NullString{
				String: val,
				Valid:  true,
			})
		}
	}

	if t.cfg.GenerationColumn != "" {
		res = append(res, sql.NullString{String: strconv.FormatUint(*t.generationID, 10), Valid: true})
	}

	return res
}

// gets row from the copy
func (t *genericTable) syncConvertStrings(fields []sql.NullString) ([]interface{}, error) {
	res := make([]interface{}, 0)
	for i, field := range fields {
		pgColName := t.pgUsedColumns[i]
		column := t.columnMapping[pgColName]

		if !field.Valid {
			if !column.IsNullable {
				return nil, fmt.Errorf("got null in %s field, which is not nullable on the ClickHouse side", pgColName)
			}
			res = append(res, nil)
			continue
		}

		val, err := convert(field.String, column, t.cfg.PgColumns[pgColName])
		if err != nil {
			return nil, fmt.Errorf("could not parse %q field with %s type: %v", pgColName, column.BaseType, err)
		}

		res = append(res, val)
	}

	return res, nil
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
