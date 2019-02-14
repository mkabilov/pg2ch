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

	"github.com/ikitiki/pg2ch/pkg/config"
	"github.com/ikitiki/pg2ch/pkg/message"
)

type commandSet [][]interface{}

type genericTable struct {
	chConn  *sql.DB
	chTx    *sql.Tx
	chStmnt *sql.Stmt

	pgTableName string
	mainTable   string
	bufferTable string

	chColumns []string
	pgColumns []string
	pg2ch     map[string]string
	chTypes   map[string]string

	bufferSize  int
	bufCmdId    int
	bufFlushCnt int
	buffer      []commandSet

	mergeThreshold int
}

func newGenericTable(conn *sql.DB, name string, tblCfg config.Table) genericTable {
	pgChColumns := make(map[string]string)
	pgChTypes := make(map[string]string)

	pgColumns := make([]string, len(tblCfg.Columns))
	chColumns := make([]string, 0)
	for colId, column := range tblCfg.Columns {
		for pgName, chProps := range column {
			pgColumns[colId] = pgName
			if chProps.ChName == "" {
				continue
			}

			pgChColumns[pgName] = chProps.ChName
			pgChTypes[pgName] = chProps.ChType
			chColumns = append(chColumns, chProps.ChName)
		}
	}

	t := genericTable{
		chConn:         conn,
		pgTableName:    name,
		mainTable:      tblCfg.MainTable,
		bufferTable:    tblCfg.BufferTable,
		pg2ch:          pgChColumns,
		chTypes:        pgChTypes,
		chColumns:      chColumns,
		pgColumns:      pgColumns,
		bufferSize:     tblCfg.BufferSize,
		mergeThreshold: tblCfg.MergeThreshold,
	}
	if tblCfg.BufferTable != "" {
		t.buffer = make([]commandSet, t.bufferSize)
	}

	return t
}

func (t *genericTable) truncate() error {
	log.Printf("truncate main table")
	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.mainTable)); err != nil {
		return fmt.Errorf("could not truncate %q table: %v", t.mainTable, err)
	}

	if err := t.truncateBufTable(); err != nil {
		return err
	}

	return nil
}

func (t *genericTable) truncateBufTable() error {
	log.Printf("truncate buf table")
	if _, err := t.chConn.Exec(fmt.Sprintf("truncate table %s", t.bufferTable)); err != nil {
		return fmt.Errorf("could not truncate %q table: %v", t.bufferTable, err)
	}

	return nil
}

func (t *genericTable) prepare() error {
	var (
		tableName string
		err       error
	)

	if t.bufferTable != "" {
		tableName = t.bufferTable
	} else {
		tableName = t.mainTable
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(t.chColumns, ", "),
		strings.Join(strings.Split(strings.Repeat("?", len(t.chColumns)), ""), ", "))

	log.Printf("prepare %q", query)

	t.chStmnt, err = t.chTx.Prepare(query)
	if err != nil {
		return fmt.Errorf("could not prepare statement: %v", err)
	}

	return nil
}

func (t *genericTable) stmntExec(params []interface{}) error {
	log.Printf("exec statement with params: %v", params)

	_, err := t.chStmnt.Exec(params...)

	return err
}

func (t *genericTable) begin() (err error) {
	t.chTx, err = t.chConn.Begin()

	log.Printf("begin")

	return
}

func (t *genericTable) genSync(pgTx *pgx.Tx, w io.Writer) error {
	columns := make([]string, 0)
	for pg := range t.pg2ch {
		columns = append(columns, pg)
	}
	query := fmt.Sprintf("copy (select %s from %s) to stdout delimiter ','", strings.Join(columns, ", "), t.pgTableName)

	if err := t.truncate(); err != nil {
		return fmt.Errorf("could not truncate tables: %v", err)
	}

	if err := t.begin(); err != nil {
		return fmt.Errorf("could not begin: %v", err)
	}

	if err := t.prepare(); err != nil {
		return fmt.Errorf("could not prepare: %v", err)
	}

	if err := pgTx.CopyToWriter(w, query); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.stmntCloseCommit(); err != nil {
		return err
	}

	if t.bufferTable != "" {
		if err := t.merge(); err != nil {
			return fmt.Errorf("could not merge: %v", err)
		}
	}

	return nil
}

func (t *genericTable) stmntCloseCommit() error {
	if err := t.chStmnt.Close(); err != nil {
		return fmt.Errorf("could not close statement: %v", err)
	}

	if err := t.chTx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %v", err)
	}

	log.Printf("stmnt close, commit")

	return nil
}

func (t *genericTable) merge() error {
	query := fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s",
		t.mainTable,
		strings.Join(t.chColumns, ","),
		t.bufferTable)
	if _, err := t.chConn.Exec(query); err != nil {
		return err
	}

	log.Printf("merge")
	return t.truncateBufTable()
}

func (t *genericTable) bufferCmdSet(set commandSet) error {
	log.Printf("buffer[%v] = %v", t.bufCmdId, set)

	t.buffer[t.bufCmdId] = set
	t.bufCmdId++
	if t.bufCmdId == t.bufferSize {
		return t.flushBuffer()
	}

	return nil
}

func (t *genericTable) flushBuffer() error {
	if t.bufCmdId == 0 {
		return nil
	}

	if err := t.begin(); err != nil {
		return err
	}

	if err := t.prepare(); err != nil {
		return err
	}

	for i := 0; i < t.bufCmdId; i++ {
		for _, cmd := range t.buffer[i] {
			if err := t.stmntExec(cmd); err != nil {
				return fmt.Errorf("could not exec: %v", err)
			}
		}
	}

	if err := t.stmntCloseCommit(); err != nil {
		return err
	}

	t.bufCmdId = 0
	t.bufFlushCnt++
	if err := t.maybeMerge(); err != nil {
		return fmt.Errorf("could not merge: %v", err)
	}

	return nil
}

func (t *genericTable) maybeMerge() error {
	if t.bufFlushCnt == 0 {
		return nil
	}

	if t.bufFlushCnt%t.mergeThreshold == 0 {
		if err := t.merge(); err != nil {
			return err
		}
	}

	return nil
}

func convert(val string, valType string) (interface{}, error) {
	switch valType {
	case "Int8":
		return strconv.ParseInt(val, 10, 8)
	case "Int16":
		return strconv.ParseInt(val, 10, 16)
	case "Int32":
		return strconv.ParseInt(val, 10, 32)
	case "Int64":
		return strconv.ParseInt(val, 10, 64)
	case "UInt8":
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
		return time.Parse("2006-01-02 15:04:05", val)
	}

	return nil, fmt.Errorf("unknown type: %v", valType)
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

		val, err := convert(string(col.Value), t.chTypes[colName])
		if err != nil {
			panic(err)
		}

		res = append(res, val)
	}

	return res
}

func (t *genericTable) convertStrings(tuples []string) []interface{} {
	res := make([]interface{}, 0)
	for i, tuple := range tuples {
		colName := t.pgColumns[i]

		val, err := convert(tuple, t.chTypes[colName])
		if err != nil {
			panic(err)
		}

		res = append(res, val)
	}

	return res
}

func (t *genericTable) Close() error {
	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffers: %v", err)
	}

	if t.bufFlushCnt > 0 {
		if err := t.merge(); err != nil {
			return fmt.Errorf("could not merge: %v", err)
		}
	}

	return nil
}
