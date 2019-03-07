package tableengines

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
)

const defaultBufferSize = 1000

type bufRow struct {
	rowID int
	data  []interface{}
}

type commandSet [][]interface{}

type bufCommand []bufRow

type genericTable struct {
	chConn  *sql.DB
	chTx    *sql.Tx
	chStmnt *sql.Stmt

	pgTableName string
	mainTable   string
	bufferTable string

	chColumns         []string
	pgColumns         []string
	pg2ch             map[string]string
	columns           map[string]config.Column
	bufferRowIdColumn string
	emptyValues       map[int]interface{}

	buffer               []bufCommand
	bufferSize           int // buffer size
	bufferCmdId          int // number of commands in the current buffer
	bufferRowId          int // row id in the buffer
	bufferFlushCnt       int // number of flushed buffers
	mergeBufferThreshold int // number of buffers before merging
	mergeQueries         []string

	syncSkip            bool
	syncSkipBufferTable bool
}

func newGenericTable(conn *sql.DB, name string, tblCfg config.Table) genericTable {
	pgChColumns := make(map[string]string)
	columns := make(map[string]config.Column)

	pgColumns := make([]string, len(tblCfg.Columns))
	chColumns := make([]string, 0)
	emptyValues := make(map[int]interface{})
	for colId, column := range tblCfg.Columns {
		for pgName, chProps := range column {
			pgColumns[colId] = pgName

			if chProps.EmptyValue != nil {
				val, err := convert(*chProps.EmptyValue, chProps)
				if err != nil {
					log.Fatalf("wrong value for %q empty value: %v", pgName, err)
				}
				emptyValues[colId] = val
			}

			if chProps.ChName == "" {
				continue
			}

			pgChColumns[pgName] = chProps.ChName
			columns[pgName] = chProps
			chColumns = append(chColumns, chProps.ChName)
		}
	}

	t := genericTable{
		chConn:               conn,
		pgTableName:          name,
		mainTable:            tblCfg.MainTable,
		bufferTable:          tblCfg.BufferTable,
		pg2ch:                pgChColumns,
		columns:              columns,
		chColumns:            chColumns,
		pgColumns:            pgColumns,
		emptyValues:          emptyValues,
		bufferSize:           tblCfg.BufferSize,
		mergeBufferThreshold: tblCfg.MergeThreshold,
		bufferRowIdColumn:    tblCfg.BufferRowIdColumn,
		syncSkip:             tblCfg.SkipInitSync,
		syncSkipBufferTable:  tblCfg.InitSyncSkipBufferTable,
	}

	if t.bufferSize < 1 {
		t.bufferSize = defaultBufferSize
	}

	t.buffer = make([]bufCommand, t.bufferSize)

	return t
}

func (t *genericTable) truncateMainTable() error {
	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.mainTable)); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateBufTable() error {
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

	columns := t.chColumns
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

	query := fmt.Sprintf("copy %s(%s) to stdout", t.pgTableName, strings.Join(t.pgColumns, ", "))

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

	if err := pgTx.CopyToWriter(w, query); err != nil {
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

		if err := t.FlushToMainTable(); err != nil {
			return fmt.Errorf("could not move from buffer to the main table: %v", err)
		}
	}

	log.Printf("Pg table %s: %d rows copied to ClickHouse", t.pgTableName, rows)

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

	if t.bufferCmdId == t.bufferSize {
		if err := t.flushBuffer(); err != nil {
			return false, fmt.Errorf("could not flush buffer: %v", err)
		}
	}

	if t.bufferTable == "" {
		return false, nil
	}

	return t.bufferFlushCnt >= t.mergeBufferThreshold, nil
}

func (t *genericTable) flushBuffer() error {
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

func (t *genericTable) FlushToMainTable() error {
	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffers: %v", err)
	}

	if t.bufferTable == "" {
		return nil
	}
	for _, query := range t.mergeQueries {
		if _, err := t.chConn.Exec(query); err != nil {
			return err
		}
	}

	t.bufferFlushCnt = 0
	t.bufferRowId = 0

	if t.bufferTable != "" {
		if err := t.truncateBufTable(); err != nil {
			return fmt.Errorf("could not truncate buffer table: %v", err)
		}
	}

	return nil
}

func convert(val string, column config.Column) (interface{}, error) {
	switch column.ChType {
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
	case "String":
		return val, nil
	case "DateTime":
		return time.Parse("2006-01-02 15:04:05", val[:19])
	}

	return nil, fmt.Errorf("unknown type: %v", column.ChType)
}

func (t *genericTable) convertTuples(row message.Row) []interface{} {
	res := make([]interface{}, 0)
	for i, col := range row {
		colName := t.pgColumns[i]

		if _, ok := t.pg2ch[colName]; !ok {
			continue
		}

		if col.Kind != message.TupleText {
			continue
		}

		val, err := convert(string(col.Value), t.columns[colName])
		if err != nil {
			panic(err)
		}

		res = append(res, val)
	}

	return res
}

func (t *genericTable) convertStrings(fields []sql.NullString) ([]interface{}, error) {
	res := make([]interface{}, 0)
	for i, field := range fields {
		pgColName := t.pgColumns[i]
		column, ok := t.columns[pgColName]
		if !ok {
			continue
		}

		if !field.Valid {
			if !column.Nullable {
				return nil, fmt.Errorf("got null in %s field, which is not nullable on the ClickHouse side", pgColName)
			}
			res = append(res, nil)
			continue
		}

		val, err := convert(field.String, column)
		if err != nil {
			return nil, fmt.Errorf("could not parse %q field with %s type: %v", pgColName, t.columns[pgColName].ChType, err)
		}

		res = append(res, val)
	}

	return res, nil
}

func (t *genericTable) Truncate() error {
	t.bufferCmdId = 0

	if err := t.truncateMainTable(); err != nil {
		return err
	}

	if t.bufferTable != "" {
		return t.truncateBufTable()
	}

	return nil
}

func (t *genericTable) Close() error {
	return t.FlushToMainTable()
}
