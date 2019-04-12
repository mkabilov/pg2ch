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
	"github.com/mkabilov/pg2ch/pkg/utils"
)

// Generic table is a "parent" struct for all the table engines
const (
	attemptInterval = time.Second
	maxAttempts     = 100
)

const (
	pgTrue  = "t"
	pgFalse = "f"
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

	cfg config.Table

	chUsedColumns  []string
	pgUsedColumns  []string
	columnMapping  map[string]config.ChColumn // [pg column name]ch column description
	emptyValues    map[int]interface{}
	flushMutex     *sync.Mutex
	buffer         []bufCommand
	bufferCmdId    int // number of commands in the current buffer
	bufferRowId    int // row id in the buffer
	bufferFlushCnt int // number of flushed buffers
	flushQueries   []string
	tupleColumns   []message.Column // Columns description taken from RELATION rep message
}

func newGenericTable(ctx context.Context, chConn *sql.DB, tblCfg config.Table) genericTable {
	t := genericTable{
		ctx:           ctx,
		chConn:        chConn,
		cfg:           tblCfg,
		columnMapping: make(map[string]config.ChColumn),
		chUsedColumns: make([]string, 0),
		pgUsedColumns: make([]string, 0),
		emptyValues:   make(map[int]interface{}),
		flushMutex:    &sync.Mutex{},
	}

	t.buffer = make([]bufCommand, t.cfg.MaxBufferLength)

	for pgColId, pgColName := range tblCfg.TupleColumns {
		chCol, ok := tblCfg.ColumnMapping[pgColName]
		if !ok {
			continue
		}

		if emptyVal, ok := tblCfg.EmptyValues[chCol.Name]; ok {
			if val, err := convert(emptyVal, chCol, t.cfg.PgColumns[pgColName]); err == nil {
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
	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.cfg.ChMainTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateBufTable() error {
	if t.cfg.ChBufferTable == "" {
		return nil
	}

	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.cfg.ChBufferTable)); err != nil {
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
	if t.cfg.ChBufferTable != "" && ((sync && !t.cfg.InitSyncSkipBufferTable) || !sync) {
		tableName = t.cfg.ChBufferTable
		columns = append(columns, t.cfg.BufferTableRowIdColumn)
	} else {
		tableName = t.cfg.ChMainTable
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
	if t.cfg.InitSyncSkip {
		return nil
	}

	if err := t.begin(); err != nil {
		return fmt.Errorf("could not begin: %v", err)
	}

	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		log.Printf("Copy from %s postgres table to %q clickhouse table via %q buffer table started",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable, t.cfg.ChBufferTable)
		if err := t.truncateBufTable(); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
	} else {
		log.Printf("Copy from %s postgres table to %q clickhouse table started",
			t.cfg.PgTableName.String(), t.cfg.ChMainTable)
		if !t.cfg.InitSyncSkipTruncate {
			if err := t.truncateMainTable(); err != nil {
				return fmt.Errorf("could not truncate main table: %v", err)
			}
		}
	}

	if err := t.stmntPrepare(true); err != nil {
		return fmt.Errorf("could not prepare: %v", err)
	}

	query := fmt.Sprintf("copy %s(%s) to stdout", t.cfg.PgTableName.String(), strings.Join(t.pgUsedColumns, ", "))
	if _, err := pgTx.CopyToWriter(w, query); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.stmntCloseCommit(); err != nil {
		return err
	}
	rows := t.bufferRowId
	t.bufferCmdId = 0
	t.bufferRowId = 0

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

	log.Printf("Pg table %s: %d rows copied to ClickHouse %q table", t.cfg.PgTableName.String(), rows, t.cfg.ChMainTable)

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

func (t *genericTable) insertRow(row []interface{}) error {
	var chTableName string

	if t.cfg.ChBufferTable != "" && !t.cfg.InitSyncSkipBufferTable {
		chTableName = t.cfg.ChBufferTable
		row = append(row, t.bufferRowId)
	} else {
		chTableName = t.cfg.ChMainTable
	}

	if err := t.stmntExec(row); err != nil {
		return fmt.Errorf("could not insert: %v", err)
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
			if t.cfg.ChBufferTable != "" {
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

	if t.cfg.ChBufferTable == "" || t.bufferFlushCnt == 0 {
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
		return strconv.ParseUint(val, 10, 32)
	case utils.ChUint64:
		return strconv.ParseUint(val, 10, 64)
	case utils.ChFloat32:
		return strconv.ParseFloat(val, 32)
	case utils.ChDecimal:
		//TODO
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

func (t *genericTable) convertTuples(row message.Row) []interface{} {
	var err error
	res := make([]interface{}, 0)

	for colId, col := range t.tupleColumns {
		var val interface{}
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if row[colId].Kind != message.TupleNull {
			val, err = convert(string(row[colId].Value), t.columnMapping[col.Name], t.cfg.PgColumns[col.Name])
			if err != nil {
				panic(err)
			}
		}

		res = append(res, val)
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

// Relation processes relation message
func (t *genericTable) Relation(rel message.Relation) {
	//TODO: suggest alter table message for adding/deleting new/old columns on clickhouse side
	t.tupleColumns = rel.Columns
}
