package tableengines

import (
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
)

// Generic table is a "parent" struct for all the table engines
const (
	defaultBufferSize = 1000
	attemptInterval   = time.Second
	maxAttempts       = 100
)

type bufRow struct {
	rowID int
	data  []interface{}
}

type commandSet [][]interface{}

type bufCommand []bufRow

type genericTable struct {
	ctx     context.Context
	chConn  *sql.DB
	chTx    *sql.Tx
	chStmnt *sql.Stmt

	pgTableName config.NamespacedName
	mainTable   string
	bufferTable string

	pgColumns []string

	chUsedColumns []string
	pgUsedColumns []string
	columnMapping map[string]config.ChColumn

	bufferRowIdColumn string
	emptyValues       map[int]interface{}

	flushMutex           *sync.Mutex
	buffer               []bufCommand
	memoryBufferSize     int // buffer size
	bufferCmdId          int // number of commands in the current buffer
	bufferRowId          int // row id in the buffer
	bufferFlushCnt       int // number of flushed buffers
	flushBufferThreshold int // number of buffers before flushing
	flushQueries         []string

	syncSkip            bool
	syncSkipBufferTable bool
}

func newGenericTable(ctx context.Context, chConn *sql.DB, name config.NamespacedName, tblCfg config.Table) genericTable {
	t := genericTable{
		ctx:                  ctx,
		chConn:               chConn,
		pgTableName:          name,
		mainTable:            tblCfg.MainTable,
		bufferTable:          tblCfg.BufferTable,
		columnMapping:        make(map[string]config.ChColumn),
		chUsedColumns:        make([]string, 0),
		pgColumns:            tblCfg.PgColumns,
		pgUsedColumns:        make([]string, 0),
		emptyValues:          make(map[int]interface{}),
		memoryBufferSize:     tblCfg.MemoryBufferSize,
		flushBufferThreshold: tblCfg.FlushThreshold,
		bufferRowIdColumn:    tblCfg.BufferTableRowIdColumn,
		syncSkip:             tblCfg.SkipInitSync,
		syncSkipBufferTable:  tblCfg.SkipBufferTable,
		flushMutex:           &sync.Mutex{},
	}

	if t.memoryBufferSize < 1 {
		t.memoryBufferSize = defaultBufferSize
	}
	t.buffer = make([]bufCommand, t.memoryBufferSize)

	for pgColId, pgColName := range tblCfg.PgColumns {
		chCol, ok := tblCfg.ColumnMapping[pgColName]
		if !ok {
			continue
		}

		if emptyVal, ok := tblCfg.EmptyValues[chCol.Name]; ok {
			if val, err := convert(emptyVal, chCol.BaseType); err == nil {
				t.emptyValues[pgColId] = val
			} else {
				log.Fatalf("wrong value for %q empty value: %v", chCol.Name, err)
			}
		}
		t.columnMapping[pgColName] = chCol
		t.chUsedColumns = append(t.chUsedColumns, chCol.Name)
		t.pgUsedColumns = append(t.pgUsedColumns, pgColName)
	}

	return t
}

func (t *genericTable) truncateMainTable() error {
	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.mainTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateBufTable() error {
	if t.bufferTable == "" {
		return nil
	}

	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.bufferTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) stmntPrepare(sync bool) error {
	var (
		tableName string
		err       error
	)

	columns := t.chUsedColumns
	if t.bufferTable != "" && ((sync && !t.syncSkipBufferTable) || !sync) {
		tableName = t.bufferTable
		columns = append(columns, t.bufferRowIdColumn)
	} else {
		tableName = t.mainTable
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(strings.Split(strings.Repeat("?", len(columns)), ""), ", "))

	t.chStmnt, err = t.chTx.Prepare(query)
	if err != nil {
		return fmt.Errorf("could not prepare statement: %v", err)
	}

	return nil
}

func (t *genericTable) stmntExec(params []interface{}) error {
	_, err := t.chStmnt.Exec(params...)

	return err
}

func (t *genericTable) begin() (err error) {
	t.chTx, err = t.chConn.Begin()

	return
}

func (t *genericTable) genSync(pgTx *pgx.Tx, w io.Writer) error {
	if t.syncSkip {
		return nil
	}

	query := fmt.Sprintf("copy %s(%s) to stdout", t.pgTableName.String(), strings.Join(t.pgUsedColumns, ", "))

	if err := t.begin(); err != nil {
		return fmt.Errorf("could not begin: %v", err)
	}

	if t.bufferTable != "" && !t.syncSkipBufferTable {
		if err := t.truncateBufTable(); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
	} else {
		if err := t.truncateMainTable(); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}
	}

	if err := t.stmntPrepare(true); err != nil {
		return fmt.Errorf("could not prepare: %v", err)
	}

	if _, err := pgTx.CopyToWriter(w, query); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.stmntCloseCommit(); err != nil {
		return err
	}
	rows := t.bufferRowId
	t.bufferCmdId = 0
	t.bufferRowId = 0

	if t.bufferTable != "" && !t.syncSkipBufferTable {
		if err := t.truncateMainTable(); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}

		t.bufferFlushCnt++
		if err := t.FlushToMainTable(); err != nil {
			return fmt.Errorf("could not move from buffer to the main table: %v", err)
		}
	}

	log.Printf("Pg table %s: %d rows copied to ClickHouse", t.pgTableName.String(), rows)

	return nil
}

func (t *genericTable) stmntCloseCommit() error {
	if err := t.chStmnt.Close(); err != nil {
		return fmt.Errorf("could not close statement: %v", err)
	}

	if err := t.chTx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %v", err)
	}

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

func (t *genericTable) processCommandSet(set commandSet) (bool, error) {
	if set != nil {
		t.bufferAppend(set)
	}

	if t.bufferCmdId == t.memoryBufferSize {
		t.flushMutex.Lock()
		defer t.flushMutex.Unlock()

		if err := t.flushBuffer(); err != nil {
			return false, fmt.Errorf("could not flush buffer: %v", err)
		}
	}

	if t.bufferTable == "" {
		return false, nil
	}

	return t.bufferFlushCnt >= t.flushBufferThreshold, nil
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

func (t *genericTable) attemptFlushBuffer() error {
	if t.bufferCmdId == 0 {
		return nil
	}

	if err := t.begin(); err != nil {
		return err
	}

	if err := t.stmntPrepare(false); err != nil {
		return err
	}

	for i := 0; i < t.bufferCmdId; i++ {
		for _, cmd := range t.buffer[i] {
			row := cmd.data
			if t.bufferTable != "" {
				row = append(row, cmd.rowID)
			}

			if err := t.stmntExec(row); err != nil {
				return fmt.Errorf("could not exec: %v", err)
			}
		}
	}

	if err := t.stmntCloseCommit(); err != nil {
		return err
	}

	t.bufferCmdId = 0
	t.bufferFlushCnt++

	return nil
}

func (t *genericTable) tryFlushToMainTable() error { //TODO: consider better name
	for _, query := range t.flushQueries {
		if _, err := t.chConn.Exec(query); err != nil {
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

	if t.bufferTable == "" || t.bufferFlushCnt == 0 {
		return nil
	}

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

func convert(val string, chType string) (interface{}, error) {
	switch chType {
	case "Int8":
		return strconv.ParseInt(val, 10, 8)
	case "Int16":
		return strconv.ParseInt(val, 10, 16)
	case "Int32":
		return strconv.ParseInt(val, 10, 32)
	case "Int64":
		return strconv.ParseInt(val, 10, 64)
	case "UInt8":
		if val == "f" { //TODO: dirty hack for boolean values, store pg types instead of ch?
			return 0, nil
		} else if val == "t" {
			return 1, nil
		}

		return strconv.ParseUint(val, 10, 8)
	case "UInt16":
		return strconv.ParseUint(val, 10, 16)
	case "UInt32":
		return strconv.ParseUint(val, 10, 32)
	case "UInt64":
		return strconv.ParseUint(val, 10, 64)
	case "Float32":
		return strconv.ParseFloat(val, 32)
	case "Float64":
		return strconv.ParseFloat(val, 64)
	case "FixedString":
		fallthrough
	case "String":
		return val, nil
	case "DateTime":
		return time.Parse("2006-01-02 15:04:05", val[:19])
	}

	return nil, fmt.Errorf("unknown type: %v", chType)
}

func (t *genericTable) convertTuples(row message.Row) []interface{} {
	res := make([]interface{}, 0)
	for i, col := range row {
		colName := t.pgColumns[i]
		if _, ok := t.columnMapping[colName]; !ok {
			continue
		}

		if col.Kind != message.TupleText {
			continue
		}

		val, err := convert(string(col.Value), t.columnMapping[colName].BaseType)
		if err != nil {
			panic(err)
		}

		res = append(res, val)
	}

	return res
}

// gets row from the copy
func (t *genericTable) convertStrings(fields []sql.NullString) ([]interface{}, error) {
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

		val, err := convert(field.String, column.BaseType)
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
